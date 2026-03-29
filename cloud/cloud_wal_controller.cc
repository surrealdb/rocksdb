//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/cloud_wal_controller.h"

#include <algorithm>
#include <cinttypes>
#include <map>
#include <set>

#include "cloud/cloud_scheduler.h"
#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "util/coding.h"

#ifdef USE_KAFKA
#include "cloud/kafka_wal.h"
#endif

namespace ROCKSDB_NAMESPACE {

// ---------------------------------------------------------------------------
// CloudWALRecord serialization
// ---------------------------------------------------------------------------

void CloudWALRecord::SerializeAppend(const Slice& filename, const Slice& data,
                                     uint64_t offset, std::string* out) {
  out->clear();
  PutVarint32(out, kAppend);
  PutFixed64(out, offset);
  PutLengthPrefixedSlice(out, filename);
  PutLengthPrefixedSlice(out, data);
}

void CloudWALRecord::SerializeClosed(const Slice& filename, uint64_t file_size,
                                     std::string* out) {
  out->clear();
  PutVarint32(out, kClosed);
  PutFixed64(out, file_size);
  PutLengthPrefixedSlice(out, filename);
}

void CloudWALRecord::SerializeDelete(const std::string& filename,
                                     std::string* out) {
  out->clear();
  PutVarint32(out, kDelete);
  PutLengthPrefixedSlice(out, Slice(filename));
}

bool CloudWALRecord::Extract(const Slice& input, uint32_t* operation,
                             Slice* filename, uint64_t* offset_in_file,
                             uint64_t* file_size, Slice* data) {
  Slice in = input;
  if (!GetVarint32(&in, operation)) return false;

  *offset_in_file = 0;
  *file_size = 0;
  *data = Slice();

  if (*operation == kAppend) {
    if (!GetFixed64(&in, offset_in_file)) return false;
    if (!GetLengthPrefixedSlice(&in, filename)) return false;
    if (!GetLengthPrefixedSlice(&in, data)) return false;
  } else if (*operation == kDelete) {
    if (!GetLengthPrefixedSlice(&in, filename)) return false;
  } else if (*operation == kClosed) {
    if (!GetFixed64(&in, file_size)) return false;
    if (!GetLengthPrefixedSlice(&in, filename)) return false;
  } else {
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
// CloudWALWritableFile
// ---------------------------------------------------------------------------

CloudWALWritableFile::CloudWALWritableFile(
    CloudFileSystem* cfs, const std::string& fname,
    const FileOptions& /*file_opts*/,
    std::unique_ptr<FSWritableFile> local_file,
#ifdef USE_KAFKA
    KafkaWALProducer* kafka_producer,
#endif
    const CloudFileSystemOptions& cloud_opts)
    : cfs_(cfs),
      fname_(fname),
      local_file_(std::move(local_file)),
#ifdef USE_KAFKA
      kafka_producer_(kafka_producer),
#endif
      kafka_sync_mode_(cloud_opts.kafka_wal_sync_mode),
      current_offset_(0),
      sync_buffer_start_offset_(0) {
  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->GetLogger(),
      "[cloud_wal] CloudWALWritableFile opened %s", fname_.c_str());
}

CloudWALWritableFile::~CloudWALWritableFile() {
  if (local_file_ != nullptr) {
    IOOptions opts;
    Close(opts, nullptr);
  }
}

IOStatus CloudWALWritableFile::Append(const Slice& data,
                                      const IOOptions& opts,
                                      IODebugContext* dbg) {
  IOStatus s = IOStatus::OK();

  if (local_file_) {
    s = local_file_->Append(data, opts, dbg);
    if (!s.ok()) return s;
  }

#ifdef USE_KAFKA
  if (kafka_sync_mode_ == WalKafkaSyncMode::kPerAppend && kafka_producer_) {
    s = kafka_producer_->Publish(fname_, data, current_offset_);
    if (!s.ok()) return s;
  } else if (kafka_sync_mode_ == WalKafkaSyncMode::kPerSync &&
             kafka_producer_) {
    sync_buffer_.append(data.data(), data.size());
  }
#endif

  current_offset_ += data.size();
  return s;
}

IOStatus CloudWALWritableFile::Flush(const IOOptions& opts,
                                     IODebugContext* dbg) {
  if (local_file_) {
    return local_file_->Flush(opts, dbg);
  }
  return IOStatus::OK();
}

IOStatus CloudWALWritableFile::Sync(const IOOptions& opts,
                                    IODebugContext* dbg) {
  IOStatus s = IOStatus::OK();

  if (local_file_) {
    s = local_file_->Sync(opts, dbg);
    if (!s.ok()) return s;
  }

#ifdef USE_KAFKA
  if (kafka_sync_mode_ == WalKafkaSyncMode::kPerSync && kafka_producer_ &&
      !sync_buffer_.empty()) {
    s = kafka_producer_->Publish(fname_, Slice(sync_buffer_),
                                 sync_buffer_start_offset_);
    if (s.ok()) {
      s = kafka_producer_->Flush();
    }
    if (!s.ok()) return s;
    sync_buffer_start_offset_ = current_offset_;
    sync_buffer_.clear();
  }
#endif

  return s;
}

IOStatus CloudWALWritableFile::Fsync(const IOOptions& opts,
                                     IODebugContext* dbg) {
  return Sync(opts, dbg);
}

IOStatus CloudWALWritableFile::Close(const IOOptions& opts,
                                     IODebugContext* dbg) {
  IOStatus s = IOStatus::OK();

  // Flush any remaining sync buffer to Kafka
#ifdef USE_KAFKA
  if (kafka_sync_mode_ != WalKafkaSyncMode::kNone && kafka_producer_) {
    if (!sync_buffer_.empty()) {
      s = kafka_producer_->Publish(fname_, Slice(sync_buffer_),
                                   sync_buffer_start_offset_);
      sync_buffer_.clear();
    }
    if (s.ok()) {
      s = kafka_producer_->PublishClosed(fname_, current_offset_);
    }
    if (s.ok()) {
      s = kafka_producer_->Flush();
    }
  }
#endif

  if (local_file_) {
    auto ls = local_file_->Close(opts, dbg);
    local_file_.reset();
    if (!ls.ok() && s.ok()) s = ls;
  }

  Log(InfoLogLevel::DEBUG_LEVEL, cfs_->GetLogger(),
      "[cloud_wal] CloudWALWritableFile closed %s size %" PRIu64,
      fname_.c_str(), current_offset_);
  return s;
}

uint64_t CloudWALWritableFile::GetFileSize(const IOOptions& opts,
                                           IODebugContext* dbg) {
  if (local_file_) {
    return local_file_->GetFileSize(opts, dbg);
  }
  return current_offset_;
}

// ---------------------------------------------------------------------------
// BackgroundWALUploader
// ---------------------------------------------------------------------------

BackgroundWALUploader::BackgroundWALUploader(CloudFileSystem* cfs,
                                             const std::string& local_dbname,
                                             uint64_t interval_ms,
                                             bool use_delta_upload)
    : cfs_(cfs),
      local_dbname_(local_dbname),
      interval_ms_(interval_ms),
      use_delta_upload_(use_delta_upload),
      job_handle_(-1),
      running_(false) {}

BackgroundWALUploader::~BackgroundWALUploader() { Stop(); }

IOStatus BackgroundWALUploader::UploadWALFile(const std::string& local_path) {
  if (!cfs_->HasDestBucket()) {
    return IOStatus::InvalidArgument("No destination bucket for WAL upload");
  }
  auto fname = basename(local_path);
  auto cloud_path = cfs_->GetDestObjectPath() + "/wal/" + fname;
  return cfs_->CopyLocalFileToDest(local_path, cloud_path);
}

IOStatus BackgroundWALUploader::UploadWALDelta(const std::string& local_path,
                                               uint64_t from_offset,
                                               uint64_t to_size) {
  if (!cfs_->HasDestBucket()) {
    return IOStatus::InvalidArgument("No destination bucket for WAL upload");
  }

  auto& base_fs = cfs_->GetBaseFileSystem();
  uint64_t delta_len = to_size - from_offset;

  std::unique_ptr<FSSequentialFile> file;
  auto s = base_fs->NewSequentialFile(local_path, FileOptions(), &file,
                                      nullptr);
  if (!s.ok()) return s;

  // Skip to the delta start offset
  if (from_offset > 0) {
    std::unique_ptr<char[]> skip_buf(new char[from_offset]);
    Slice skip_result;
    s = file->Read(from_offset, IOOptions(), &skip_result, skip_buf.get(),
                   nullptr);
    if (!s.ok()) return s;
  }

  // Read the delta bytes
  std::string delta_data;
  delta_data.resize(delta_len);
  Slice read_result;
  s = file->Read(delta_len, IOOptions(), &read_result,
                 delta_data.data(), nullptr);
  if (!s.ok()) return s;
  delta_data.resize(read_result.size());

  // Write delta to a temp file and upload
  auto fname = basename(local_path);
  std::string delta_suffix = ".delta." + std::to_string(from_offset);
  auto cloud_path =
      cfs_->GetDestObjectPath() + "/wal/" + fname + delta_suffix;

  std::string tmp_path = local_path + delta_suffix + ".tmp";
  {
    std::unique_ptr<FSWritableFile> tmp_file;
    s = base_fs->NewWritableFile(tmp_path, FileOptions(), &tmp_file, nullptr);
    if (!s.ok()) return s;
    s = tmp_file->Append(Slice(delta_data), IOOptions(), nullptr);
    if (!s.ok()) {
      base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
      return s;
    }
    s = tmp_file->Close(IOOptions(), nullptr);
    if (!s.ok()) {
      base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
      return s;
    }
  }

  s = cfs_->CopyLocalFileToDest(tmp_path, cloud_path);
  base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);

  if (s.ok()) {
    Log(InfoLogLevel::DEBUG_LEVEL, cfs_->GetLogger(),
        "[cloud_wal] Uploaded delta %s offset %" PRIu64 " len %" PRIu64,
        fname.c_str(), from_offset, delta_len);
  }
  return s;
}

void BackgroundWALUploader::DoUpload(void* /*arg*/) {
  if (!running_.load(std::memory_order_relaxed)) return;
  DoUploadImpl();
}

void BackgroundWALUploader::DoUploadImpl() {
  auto& base_fs = cfs_->GetBaseFileSystem();
  std::vector<std::string> children;
  auto st =
      base_fs->GetChildren(local_dbname_, IOOptions(), &children, nullptr);
  if (!st.ok()) return;

  std::set<std::string> local_wal_files;
  for (const auto& child : children) {
    if (!IsWalFile(child)) continue;
    local_wal_files.insert(child);
    auto local_path = local_dbname_ + "/" + child;

    uint64_t local_size = 0;
    auto ss = base_fs->GetFileSize(local_path, IOOptions(), &local_size,
                                   nullptr);
    if (!ss.ok()) continue;

    uint64_t last_uploaded = 0;
    {
      std::lock_guard<std::mutex> lk(mu_);
      auto it = uploaded_sizes_.find(child);
      if (it != uploaded_sizes_.end()) last_uploaded = it->second;
    }

    if (local_size == last_uploaded) continue;

    IOStatus s;
    if (use_delta_upload_) {
      s = UploadWALDelta(local_path, last_uploaded, local_size);
    } else {
      s = UploadWALFile(local_path);
    }

    if (s.ok()) {
      std::lock_guard<std::mutex> lk(mu_);
      uploaded_sizes_[child] = local_size;
    } else {
      Log(InfoLogLevel::WARN_LEVEL, cfs_->GetLogger(),
          "[cloud_wal] Background WAL upload failed for %s: %s",
          local_path.c_str(), s.ToString().c_str());
    }
  }

  // Clean up obsolete cloud objects for WAL files no longer present locally
  if (cfs_->HasDestBucket()) {
    auto provider = cfs_->GetStorageProvider();
    std::string wal_prefix = cfs_->GetDestObjectPath() + "/wal/";
    std::vector<std::string> cloud_wals;
    auto ls = provider->ListCloudObjects(cfs_->GetDestBucketName(),
                                         wal_prefix, &cloud_wals);
    if (ls.ok()) {
      for (const auto& cloud_wal : cloud_wals) {
        // Extract the base WAL name (strip .delta.NNN suffix if present)
        std::string base_name = cloud_wal;
        auto delta_pos = base_name.find(".delta.");
        if (delta_pos != std::string::npos) {
          base_name = base_name.substr(0, delta_pos);
        }
        if (!IsWalFile(base_name)) continue;
        if (local_wal_files.find(base_name) == local_wal_files.end()) {
          auto cloud_path = wal_prefix + cloud_wal;
          auto ds = provider->DeleteCloudObject(cfs_->GetDestBucketName(),
                                                cloud_path);
          if (ds.ok()) {
            Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
                "[cloud_wal] Deleted obsolete S3 WAL %s", cloud_path.c_str());
          }
        }
      }
    }

    // Remove tracking entries for deleted WAL files
    {
      std::lock_guard<std::mutex> lk(mu_);
      for (auto it = uploaded_sizes_.begin(); it != uploaded_sizes_.end();) {
        if (local_wal_files.find(it->first) == local_wal_files.end()) {
          it = uploaded_sizes_.erase(it);
        } else {
          ++it;
        }
      }
    }
  }
}

void BackgroundWALUploader::Start() {
  if (running_.exchange(true)) return;
  scheduler_ = CloudScheduler::Get();
  auto freq = std::chrono::milliseconds(interval_ms_);
  job_handle_ = scheduler_->ScheduleRecurringJob(
      std::chrono::microseconds(freq), std::chrono::microseconds(freq),
      [](void* arg) { static_cast<BackgroundWALUploader*>(arg)->DoUpload(arg); },
      this);
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[cloud_wal] Background WAL uploader started, interval %" PRIu64
      "ms delta=%d",
      interval_ms_, use_delta_upload_);
}

void BackgroundWALUploader::Stop() {
  if (!running_.exchange(false)) return;
  if (scheduler_ && job_handle_ >= 0) {
    scheduler_->CancelJob(job_handle_);
    job_handle_ = -1;
  }
  DoUploadImpl();
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[cloud_wal] Background WAL uploader stopped");
}

// ---------------------------------------------------------------------------
// CloudWALController
// ---------------------------------------------------------------------------

CloudWALController::CloudWALController(
    CloudFileSystem* cfs, const std::shared_ptr<FileSystem>& base_fs,
    const CloudFileSystemOptions& opts, const std::shared_ptr<Logger>& logger)
    : cfs_(cfs), base_fs_(base_fs), cloud_opts_(opts), info_log_(logger) {
  active_ = (opts.kafka_wal_sync_mode != WalKafkaSyncMode::kNone) ||
            opts.background_wal_sync_to_cloud ||
            !opts.keep_local_log_files;

#ifdef USE_KAFKA
  if (opts.kafka_wal_sync_mode != WalKafkaSyncMode::kNone) {
    std::string topic = opts.kafka_topic_prefix + "." +
                        opts.dest_bucket.GetBucketName();
    kafka_producer_ = std::make_unique<KafkaWALProducer>(
        opts.kafka_bootstrap_servers, topic, logger);
    auto s = kafka_producer_->Initialize();
    if (!s.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, logger,
          "[cloud_wal] Failed to initialize Kafka producer: %s",
          s.ToString().c_str());
      kafka_producer_.reset();
    }
  }
#endif

  if (active_) {
    Log(InfoLogLevel::INFO_LEVEL, logger,
        "[cloud_wal] CloudWALController initialized. keep_local=%d "
        "kafka_mode=%d bg_sync=%d",
        opts.keep_local_log_files,
        static_cast<int>(opts.kafka_wal_sync_mode),
        opts.background_wal_sync_to_cloud);
  }
}

CloudWALController::~CloudWALController() { Stop(); }

void CloudWALController::Stop() {
  if (bg_uploader_) {
    bg_uploader_->Stop();
    bg_uploader_.reset();
  }
#ifdef USE_KAFKA
  kafka_producer_.reset();
#endif
}

void CloudWALController::StartBackgroundUploader(
    const std::string& local_dbname) {
  if (cloud_opts_.background_wal_sync_to_cloud && !bg_uploader_) {
    bg_uploader_ = std::make_unique<BackgroundWALUploader>(
        cfs_, local_dbname, cloud_opts_.background_wal_sync_interval_ms,
        cloud_opts_.use_wal_delta_upload);
    bg_uploader_->Start();
  }
}

IOStatus CloudWALController::NewWritableFile(
    const std::string& fname, const FileOptions& file_opts,
    std::unique_ptr<FSWritableFile>* result, IODebugContext* dbg) {
  std::unique_ptr<FSWritableFile> local_file;

  if (cloud_opts_.keep_local_log_files) {
    auto s = base_fs_->NewWritableFile(fname, file_opts, &local_file, dbg);
    if (!s.ok()) {
      return s;
    }
  }

  result->reset(new CloudWALWritableFile(
      cfs_, fname, file_opts, std::move(local_file),
#ifdef USE_KAFKA
      kafka_producer_.get(),
#endif
      cloud_opts_));
  return IOStatus::OK();
}

IOStatus CloudWALController::RecoverWALFromCloud(
    const std::string& local_dbname) {
  if (!cloud_opts_.background_wal_sync_to_cloud || !cfs_->HasDestBucket()) {
    return IOStatus::OK();
  }

  auto provider = cfs_->GetStorageProvider();
  if (!provider) {
    return IOStatus::InvalidArgument("No storage provider for WAL recovery");
  }

  std::string wal_prefix = cfs_->GetDestObjectPath() + "/wal/";
  std::vector<std::string> wal_objects;
  auto st = provider->ListCloudObjects(cfs_->GetDestBucketName(), wal_prefix,
                                       &wal_objects);
  if (!st.ok()) {
    if (st.IsNotFound()) return IOStatus::OK();
    return st;
  }

  // Separate whole-file WALs from delta chunks and group deltas by base name
  std::set<std::string> whole_wal_files;
  // base_name -> sorted list of (offset, cloud_object_name)
  std::map<std::string, std::vector<std::pair<uint64_t, std::string>>>
      delta_groups;

  for (const auto& obj : wal_objects) {
    auto delta_pos = obj.find(".delta.");
    if (delta_pos != std::string::npos) {
      std::string base_name = obj.substr(0, delta_pos);
      std::string offset_str = obj.substr(delta_pos + 7);  // len(".delta.") = 7
      uint64_t offset = 0;
      try {
        offset = std::stoull(offset_str);
      } catch (...) {
        continue;
      }
      delta_groups[base_name].emplace_back(offset, obj);
    } else if (IsWalFile(obj)) {
      whole_wal_files.insert(obj);
    }
  }

  // Download whole-file WALs (skip any that have delta chunks)
  for (const auto& obj : whole_wal_files) {
    if (delta_groups.count(obj) > 0) continue;
    auto local_path = local_dbname + "/" + obj;
    auto cloud_path = wal_prefix + obj;
    st = provider->GetCloudObject(cfs_->GetDestBucketName(), cloud_path,
                                  local_path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[cloud_wal] Failed to download WAL %s: %s", cloud_path.c_str(),
          st.ToString().c_str());
    } else if (st.ok()) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_wal] Recovered WAL file %s from cloud", obj.c_str());
    }
  }

  // Reassemble delta groups into local WAL files
  auto& base_fs = cfs_->GetBaseFileSystem();
  for (auto& kv : delta_groups) {
    const auto& base_name = kv.first;
    auto& deltas = kv.second;

    std::sort(deltas.begin(), deltas.end());

    auto local_path = local_dbname + "/" + base_name;
    std::unique_ptr<FSWritableFile> local_file;
    st = base_fs->NewWritableFile(local_path, FileOptions(), &local_file,
                                  nullptr);
    if (!st.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[cloud_wal] Failed to create local WAL %s for delta reassembly: %s",
          local_path.c_str(), st.ToString().c_str());
      continue;
    }

    bool reassembly_ok = true;
    for (const auto& delta : deltas) {
      auto cloud_path = wal_prefix + delta.second;
      std::string tmp_path = local_path + ".delta_tmp";
      st = provider->GetCloudObject(cfs_->GetDestBucketName(), cloud_path,
                                    tmp_path);
      if (!st.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[cloud_wal] Failed to download delta %s: %s",
            cloud_path.c_str(), st.ToString().c_str());
        reassembly_ok = false;
        break;
      }

      uint64_t chunk_size = 0;
      st = base_fs->GetFileSize(tmp_path, IOOptions(), &chunk_size, nullptr);
      if (!st.ok() || chunk_size == 0) {
        base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
        continue;
      }

      std::unique_ptr<FSSequentialFile> chunk_file;
      st = base_fs->NewSequentialFile(tmp_path, FileOptions(), &chunk_file,
                                      nullptr);
      if (!st.ok()) {
        base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
        reassembly_ok = false;
        break;
      }

      std::string chunk_data;
      chunk_data.resize(chunk_size);
      Slice chunk_result;
      st = chunk_file->Read(chunk_size, IOOptions(), &chunk_result,
                            chunk_data.data(), nullptr);
      if (!st.ok()) {
        base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
        reassembly_ok = false;
        break;
      }

      st = local_file->Append(chunk_result, IOOptions(), nullptr);
      base_fs->DeleteFile(tmp_path, IOOptions(), nullptr);
      if (!st.ok()) {
        reassembly_ok = false;
        break;
      }
    }

    auto cs = local_file->Close(IOOptions(), nullptr);
    if (!cs.ok() && reassembly_ok) reassembly_ok = false;

    if (reassembly_ok) {
      Log(InfoLogLevel::INFO_LEVEL, info_log_,
          "[cloud_wal] Recovered WAL %s from %zu delta chunks",
          base_name.c_str(), deltas.size());
    } else {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[cloud_wal] Failed to reassemble WAL %s from deltas",
          base_name.c_str());
    }
  }

  return IOStatus::OK();
}

IOStatus CloudWALController::RecoverWALFromKafka(
    const std::string& local_dbname) {
#ifdef USE_KAFKA
  if (cloud_opts_.kafka_wal_sync_mode == WalKafkaSyncMode::kNone) {
    return IOStatus::OK();
  }

  std::string topic = cloud_opts_.kafka_topic_prefix + "." +
                      cloud_opts_.dest_bucket.GetBucketName();

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_wal] Recovering WAL from Kafka topic %s into %s",
      topic.c_str(), local_dbname.c_str());

  KafkaWALTailer tailer(cloud_opts_.kafka_bootstrap_servers, topic,
                        local_dbname, base_fs_, info_log_);
  auto st = tailer.ReplayAll();
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[cloud_wal] Kafka WAL recovery failed: %s", st.ToString().c_str());
    return st;
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[cloud_wal] Kafka WAL recovery complete for topic %s", topic.c_str());
  return IOStatus::OK();
#else
  (void)local_dbname;
  return IOStatus::OK();
#endif
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
