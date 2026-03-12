//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/cloud_wal_controller.h"

#include <cinttypes>

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
    sync_buffer_start_offset_ = current_offset_;
    sync_buffer_.clear();
    if (!s.ok()) return s;
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
                                             uint64_t interval_ms)
    : cfs_(cfs),
      local_dbname_(local_dbname),
      interval_ms_(interval_ms),
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

void BackgroundWALUploader::DoUpload(void* /*arg*/) {
  if (!running_.load(std::memory_order_relaxed)) return;

  auto& base_fs = cfs_->GetBaseFileSystem();
  std::vector<std::string> children;
  auto st =
      base_fs->GetChildren(local_dbname_, IOOptions(), &children, nullptr);
  if (!st.ok()) return;

  for (const auto& child : children) {
    if (!IsWalFile(child)) continue;
    auto local_path = local_dbname_ + "/" + child;
    auto s = UploadWALFile(local_path);
    if (!s.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, cfs_->GetLogger(),
          "[cloud_wal] Background WAL upload failed for %s: %s",
          local_path.c_str(), s.ToString().c_str());
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
      "[cloud_wal] Background WAL uploader started, interval %" PRIu64 "ms",
      interval_ms_);
}

void BackgroundWALUploader::Stop() {
  if (!running_.exchange(false)) return;
  if (scheduler_ && job_handle_ >= 0) {
    scheduler_->CancelJob(job_handle_);
    job_handle_ = -1;
  }
  // Final upload of all WAL files
  DoUpload(nullptr);
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
        cfs_, local_dbname, cloud_opts_.background_wal_sync_interval_ms);
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

  std::string wal_prefix =
      cfs_->GetDestObjectPath() + "/wal/";
  std::vector<std::string> wal_objects;
  auto st = provider->ListCloudObjects(cfs_->GetDestBucketName(), wal_prefix,
                                       &wal_objects);
  if (!st.ok()) {
    if (st.IsNotFound()) return IOStatus::OK();
    return st;
  }

  for (const auto& obj : wal_objects) {
    if (!IsWalFile(obj)) continue;
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

  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
