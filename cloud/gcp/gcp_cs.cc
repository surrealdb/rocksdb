// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE
#ifdef USE_GCS
#include "google/cloud/storage/bucket_metadata.h"
#include "google/cloud/storage/client.h"
#endif

#include "cloud/filename.h"
#include "cloud/gcp/gcp_file_system.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/convenience.h"

#include <inttypes.h>
#include <fstream>

#ifdef _WIN32_WINNT
#undef GetMessage
#endif

namespace ROCKSDB_NAMESPACE {
#ifdef USE_GCS

namespace gcs = ::google::cloud::storage;
namespace gcp = ::google::cloud;

static bool IsNotFound(const gcp::Status& status) {
  return (status.code() == gcp::StatusCode::kNotFound);
}

// GCS does not collapse successive slashes like S3 does.
// Normalize by reducing multiple slashes to one and stripping leading slash.
inline std::string normalize_object_path(const std::string& object_path) {
  std::string path;
  path.reserve(object_path.size());
  bool prev_slash = false;
  for (char c : object_path) {
    if (c == '/') {
      if (!prev_slash) {
        path.push_back(c);
      }
      prev_slash = true;
    } else {
      path.push_back(c);
      prev_slash = false;
    }
  }
  if (!path.empty() && path[0] == '/') {
    path.erase(0, 1);
  }
  return path;
}

class CloudRequestCallbackGuard {
 public:
  CloudRequestCallbackGuard(CloudRequestCallback* callback,
                            CloudRequestOpType type, uint64_t size = 0)
      : callback_(callback), type_(type), size_(size), start_(now()) {}

  ~CloudRequestCallbackGuard() {
    if (callback_) {
      (*callback_)(type_, size_, now() - start_, success_);
    }
  }

  void SetSize(uint64_t size) { size_ = size; }
  void SetSuccess(bool success) { success_ = success; }

 private:
  uint64_t now() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::system_clock::now() -
               std::chrono::system_clock::from_time_t(0))
        .count();
  }
  CloudRequestCallback* callback_;
  CloudRequestOpType type_;
  uint64_t size_;
  bool success_{false};
  uint64_t start_;
};

/******************** GCSClientWrapper ******************/

class GCSClientWrapper {
 public:
  explicit GCSClientWrapper(const CloudFileSystemOptions& cloud_options,
                            gcp::Options gcp_options)
      : cloud_request_callback_(cloud_options.cloud_request_callback) {
    if (cloud_options.gcs_client_factory) {
      client_ = cloud_options.gcs_client_factory(gcp_options);
    } else {
      client_ = std::make_shared<gcs::Client>(gcp_options);
    }
  }

  gcp::StatusOr<gcs::BucketMetadata> CreateBucket(
      const std::string& bucket_name, gcs::BucketMetadata metadata) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCreateOp);
    auto result = client_->CreateBucket(bucket_name, metadata);
    t.SetSuccess(result.ok());
    return result;
  }

  gcs::ListObjectsReader ListCloudObjects(
      const std::string& bucket_name, const std::string& prefix) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kListOp);
    auto result =
        client_->ListObjects(bucket_name, gcs::Prefix(prefix));
    t.SetSuccess(true);
    return result;
  }

  gcp::StatusOr<gcs::BucketMetadata> HeadBucket(
      const std::string& bucket_name) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    auto result = client_->GetBucketMetadata(bucket_name);
    t.SetSuccess(result.ok());
    return result;
  }

  gcp::Status DeleteCloudObject(const std::string& bucket_name,
                                const std::string& object_path) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kDeleteOp);
    auto result = client_->DeleteObject(bucket_name, object_path);
    t.SetSuccess(result.ok());
    return result;
  }

  gcp::StatusOr<gcs::ObjectMetadata> CopyCloudObject(
      const std::string& src_bucket, const std::string& src_object,
      const std::string& dst_bucket, const std::string& dst_object) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kCopyOp);
    auto result =
        client_->CopyObject(src_bucket, src_object, dst_bucket, dst_object);
    t.SetSuccess(result.ok());
    return result;
  }

  gcp::Status GetCloudObject(const std::string& bucket,
                             const std::string& object, int64_t start,
                             size_t n, char* buf, uint64_t* bytes_read) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kReadOp);
    // Ranges are inclusive, so we can't read 0 bytes; read 1 instead
    // and drop it later.
    size_t range_len = (n != 0 ? n : 1);
    uint64_t end = start + range_len;
    *bytes_read = 0;

    gcs::ObjectReadStream obj =
        client_->ReadObject(bucket, object, gcs::ReadRange(start, end));
    if (obj.bad()) {
      return obj.status();
    }

    if (n != 0) {
      obj.read(buf, n);
      *bytes_read = obj.gcount();
      assert(*bytes_read <= n);
    }

    t.SetSize(*bytes_read);
    t.SetSuccess(true);
    return obj.status();
  }

  gcp::Status DownloadFile(const std::string& bucket_name,
                           const std::string& object_path,
                           const std::string& dst_file,
                           uint64_t* file_size) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kReadOp);

    gcs::ObjectReadStream os = client_->ReadObject(bucket_name, object_path);
    if (os.bad()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      return os.status();
    }

    std::ofstream ofs(dst_file, std::ofstream::binary);
    if (!ofs.is_open()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      std::string errmsg("Unable to open dest file ");
      errmsg.append(dst_file);
      return gcp::Status(gcp::StatusCode::kInternal, errmsg);
    }

    ofs << os.rdbuf();
    ofs.close();
    *file_size = os.size().value();
    guard.SetSize(*file_size);
    guard.SetSuccess(true);
    return gcp::Status(gcp::StatusCode::kOk, "OK");
  }

  gcp::StatusOr<gcs::ObjectMetadata> PutCloudObject(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata,
      uint64_t size_hint = 0) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kWriteOp, size_hint);
    auto object_meta = client_->InsertObject(bucket_name, object_path, "");
    if (!object_meta.ok()) {
      t.SetSuccess(false);
      return object_meta;
    }
    gcs::ObjectMetadata new_object_meta = object_meta.value();
    for (const auto& kv : metadata) {
      new_object_meta.mutable_metadata().emplace(kv.first, kv.second);
    }
    auto update_meta =
        client_->UpdateObject(bucket_name, object_path, new_object_meta);
    return update_meta;
  }

  gcp::StatusOr<gcs::ObjectMetadata> UploadFile(
      const std::string& bucket_name, const std::string& object_path,
      const std::string& local_file) {
    CloudRequestCallbackGuard guard(cloud_request_callback_.get(),
                                    CloudRequestOpType::kWriteOp);

    auto result = client_->UploadFile(local_file, bucket_name, object_path);
    if (!result.ok()) {
      guard.SetSize(0);
      guard.SetSuccess(false);
      return result;
    }

    guard.SetSize(result.value().size());
    guard.SetSuccess(true);
    return result;
  }

  gcp::StatusOr<gcs::ObjectMetadata> HeadObject(
      const std::string& bucket_name, const std::string& object_path) {
    CloudRequestCallbackGuard t(cloud_request_callback_.get(),
                                CloudRequestOpType::kInfoOp);
    auto result = client_->GetObjectMetadata(bucket_name, object_path);
    t.SetSuccess(result.ok());
    return result;
  }

  CloudRequestCallback* GetRequestCallback() {
    return cloud_request_callback_.get();
  }

 private:
  std::shared_ptr<google::cloud::storage::Client> client_;
  std::shared_ptr<CloudRequestCallback> cloud_request_callback_;
};

/******************** GcsReadableFile ******************/

class GcsReadableFile : public CloudStorageReadableFileImpl {
 public:
  GcsReadableFile(const std::shared_ptr<GCSClientWrapper>& gcs_client,
                  Logger* info_log, const std::string& bucket,
                  const std::string& fname, uint64_t size,
                  std::string content_hash)
      : CloudStorageReadableFileImpl(info_log, bucket, fname, size),
        gcs_client_(gcs_client),
        content_hash_(std::move(content_hash)) {}

  virtual const char* Type() const { return "gcs"; }

  size_t GetUniqueId(char* id, size_t max_size) const override {
    if (content_hash_.empty()) {
      return 0;
    }
    max_size = std::min(content_hash_.size(), max_size);
    memcpy(id, content_hash_.c_str(), max_size);
    return max_size;
  }

  IOStatus DoCloudRead(uint64_t offset, size_t n,
                       const IOOptions& /*options*/, char* scratch,
                       uint64_t* bytes_read,
                       IODebugContext* /*dbg*/) const override {
    auto status = gcs_client_->GetCloudObject(bucket_, fname_, offset, n,
                                              scratch, bytes_read);
    if (!status.ok()) {
      if (IsNotFound(status)) {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[gcs] GcsReadableFile ReadObject Not Found %s\n", fname_.c_str());
        return IOStatus::NotFound();
      } else {
        Log(InfoLogLevel::ERROR_LEVEL, info_log_,
            "[gcs] GcsReadableFile ReadObject error %s offset %" PRIu64
            " rangelen %zu, message: %s\n",
            fname_.c_str(), offset, n, status.message().c_str());
        return IOStatus::IOError(fname_.c_str(), status.message().c_str());
      }
    }
    return IOStatus::OK();
  }

 private:
  std::shared_ptr<GCSClientWrapper> gcs_client_;
  std::string content_hash_;
};

/******************** GcsWritableFile ******************/

class GcsWritableFile : public CloudStorageWritableFileImpl {
 public:
  GcsWritableFile(CloudFileSystem* fs, const std::string& local_fname,
                  const std::string& bucket, const std::string& cloud_fname,
                  const FileOptions& options)
      : CloudStorageWritableFileImpl(fs, local_fname, bucket, cloud_fname,
                                     options) {}
  const char* Name() const override {
    return CloudStorageProviderImpl::kGcs();
  }
};

/******************** GcsStorageProvider ******************/

struct HeadObjectResult {
  std::unordered_map<std::string, std::string>* metadata = nullptr;
  uint64_t* size = nullptr;
  uint64_t* modtime = nullptr;
  std::string* etag = nullptr;
};

class GcsStorageProvider : public CloudStorageProviderImpl {
 public:
  ~GcsStorageProvider() override {}
  const char* Name() const override { return kGcs(); }
  IOStatus CreateBucket(const std::string& bucket) override;
  IOStatus ExistsBucket(const std::string& bucket) override;
  IOStatus EmptyBucket(const std::string& bucket_name,
                       const std::string& object_path) override;
  IOStatus DeleteCloudObject(const std::string& bucket_name,
                             const std::string& object_path) override;
  IOStatus ListCloudObjects(const std::string& bucket_name,
                            const std::string& object_path,
                            std::vector<std::string>* result) override;
  IOStatus ExistsCloudObject(const std::string& bucket_name,
                             const std::string& object_path) override;
  IOStatus GetCloudObjectSize(const std::string& bucket_name,
                              const std::string& object_path,
                              uint64_t* filesize) override;
  IOStatus GetCloudObjectModificationTime(const std::string& bucket_name,
                                          const std::string& object_path,
                                          uint64_t* time) override;
  IOStatus GetCloudObjectMetadata(const std::string& bucket_name,
                                  const std::string& object_path,
                                  CloudObjectInformation* info) override;
  IOStatus PutCloudObjectMetadata(
      const std::string& bucket_name, const std::string& object_path,
      const std::unordered_map<std::string, std::string>& metadata) override;
  IOStatus CopyCloudObject(const std::string& bucket_name_src,
                           const std::string& object_path_src,
                           const std::string& bucket_name_dest,
                           const std::string& object_path_dest) override;
  IOStatus DoNewCloudReadableFile(
      const std::string& bucket, const std::string& fname, uint64_t fsize,
      const std::string& content_hash, const FileOptions& options,
      std::unique_ptr<CloudStorageReadableFile>* result,
      IODebugContext* dbg) override;
  IOStatus NewCloudWritableFile(
      const std::string& local_path, const std::string& bucket_name,
      const std::string& object_path, const FileOptions& options,
      std::unique_ptr<CloudStorageWritableFile>* result,
      IODebugContext* dbg) override;
  Status PrepareOptions(const ConfigOptions& options) override;

 protected:
  IOStatus DoGetCloudObject(const std::string& bucket_name,
                            const std::string& object_path,
                            const std::string& destination,
                            uint64_t* remote_size) override;
  IOStatus DoPutCloudObject(const std::string& local_file,
                            const std::string& bucket_name,
                            const std::string& object_path,
                            uint64_t file_size,
                            const PutObjectOptions& options = {}) override;

 private:
  IOStatus HeadObject(const std::string& bucket, const std::string& path,
                      HeadObjectResult* result);

  std::shared_ptr<GCSClientWrapper> gcs_client_;
};

/******************** GcsStorageProvider Implementation ******************/

IOStatus GcsStorageProvider::CreateBucket(const std::string& bucket) {
  auto project_id = std::getenv("GOOGLE_CLOUD_PROJECT");
  if (project_id == nullptr) {
    return IOStatus::InvalidArgument(
        "GOOGLE_CLOUD_PROJECT environment variable is not set");
  }
  gcs::BucketMetadata metadata;
  metadata.set_name(bucket);
  auto outcome = gcs_client_->CreateBucket(bucket, metadata);
  if (!outcome.ok()) {
    const auto& error = outcome.status().message();
    std::string errmsg(error.data(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[gcs] CreateBucket %s error %s", bucket.c_str(), errmsg.c_str());
    return IOStatus::IOError(bucket, errmsg);
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ExistsBucket(const std::string& bucket) {
  auto outcome = gcs_client_->HeadBucket(bucket);
  if (!outcome.ok()) {
    if (IsNotFound(outcome.status())) {
      return IOStatus::NotFound(bucket, "Bucket not found");
    }
    return IOStatus::IOError(bucket, outcome.status().message());
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::EmptyBucket(const std::string& bucket_name,
                                         const std::string& object_path) {
  auto normalized_path = normalize_object_path(object_path);
  std::vector<std::string> results;
  auto s = ListCloudObjects(bucket_name, normalized_path, &results);
  if (!s.ok()) {
    return s;
  }
  for (const auto& obj : results) {
    s = DeleteCloudObject(bucket_name, normalized_path + obj);
    if (!s.ok()) {
      return s;
    }
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::DeleteCloudObject(
    const std::string& bucket_name, const std::string& object_path) {
  auto normalized_path = normalize_object_path(object_path);
  auto outcome = gcs_client_->DeleteCloudObject(bucket_name, normalized_path);
  if (!outcome.ok() && !IsNotFound(outcome)) {
    const auto& error = outcome.message();
    std::string errmsg(error.data(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[gcs] DeleteCloudObject %s/%s error %s", bucket_name.c_str(),
        object_path.c_str(), errmsg.c_str());
    return IOStatus::IOError(object_path, errmsg);
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ListCloudObjects(
    const std::string& bucket_name, const std::string& object_path,
    std::vector<std::string>* result) {
  auto normalized_path = normalize_object_path(object_path);
  std::string prefix = ensure_ends_with_pathsep(normalized_path);
  auto reader = gcs_client_->ListCloudObjects(bucket_name, prefix);
  for (auto&& obj_meta : reader) {
    if (!obj_meta.ok()) {
      const auto& error = obj_meta.status().message();
      std::string errmsg(error.data(), error.size());
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[gcs] ListCloudObjects %s/%s iteration error %s",
          bucket_name.c_str(), object_path.c_str(), errmsg.c_str());
      return IOStatus::IOError(object_path, errmsg);
    }
    auto name = obj_meta->name();
    if (name.find(prefix) != 0) {
      return IOStatus::IOError(
          "Unexpected result from Gcs: " + name);
    }
    auto fname = name.substr(prefix.size());
    result->push_back(std::move(fname));
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::ExistsCloudObject(
    const std::string& bucket_name, const std::string& object_path) {
  HeadObjectResult result;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectSize(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* filesize) {
  HeadObjectResult result;
  result.size = filesize;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectModificationTime(
    const std::string& bucket_name, const std::string& object_path,
    uint64_t* time) {
  HeadObjectResult result;
  result.modtime = time;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::GetCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    CloudObjectInformation* info) {
  assert(info != nullptr);
  HeadObjectResult result;
  result.metadata = &info->metadata;
  result.size = &info->size;
  result.modtime = &info->modification_time;
  result.etag = &info->content_hash;
  return HeadObject(bucket_name, object_path, &result);
}

IOStatus GcsStorageProvider::PutCloudObjectMetadata(
    const std::string& bucket_name, const std::string& object_path,
    const std::unordered_map<std::string, std::string>& metadata) {
  auto normalized_path = normalize_object_path(object_path);
  auto outcome =
      gcs_client_->PutCloudObject(bucket_name, normalized_path, metadata);
  if (!outcome.ok()) {
    const auto& error = outcome.status().message();
    std::string errmsg(error.data(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[gcs] Bucket %s error in saving metadata %s", bucket_name.c_str(),
        errmsg.c_str());
    return IOStatus::IOError(object_path, errmsg);
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::CopyCloudObject(
    const std::string& bucket_name_src, const std::string& object_path_src,
    const std::string& bucket_name_dest,
    const std::string& object_path_dest) {
  std::string src_url = bucket_name_src + object_path_src;
  auto normalized_src = normalize_object_path(object_path_src);
  auto normalized_dest = normalize_object_path(object_path_dest);
  auto copy = gcs_client_->CopyCloudObject(bucket_name_src, normalized_src,
                                            bucket_name_dest, normalized_dest);
  if (!copy.ok()) {
    const auto& error = copy.status().message();
    std::string errmsg(error.data(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[gcs] CopyCloudObject src %s error copying to %s %s",
        src_url.c_str(), object_path_dest.c_str(), errmsg.c_str());
    return IOStatus::IOError(object_path_dest, errmsg);
  }
  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[gcs] CopyCloudObject src %s copied to %s OK", src_url.c_str(),
      object_path_dest.c_str());
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::DoNewCloudReadableFile(
    const std::string& bucket, const std::string& fname, uint64_t fsize,
    const std::string& content_hash, const FileOptions& /*options*/,
    std::unique_ptr<CloudStorageReadableFile>* result,
    IODebugContext* /*dbg*/) {
  auto normalized_path = normalize_object_path(fname);
  result->reset(new GcsReadableFile(gcs_client_, cfs_->GetLogger(), bucket,
                                    normalized_path, fsize, content_hash));
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::NewCloudWritableFile(
    const std::string& local_path, const std::string& bucket_name,
    const std::string& object_path, const FileOptions& file_opts,
    std::unique_ptr<CloudStorageWritableFile>* result,
    IODebugContext* /*dbg*/) {
  auto normalized_path = normalize_object_path(object_path);
  result->reset(new GcsWritableFile(cfs_, local_path, bucket_name,
                                    normalized_path, file_opts));
  return (*result)->status();
}

Status GcsStorageProvider::PrepareOptions(const ConfigOptions& options) {
  auto cfs = dynamic_cast<CloudFileSystem*>(options.env->GetFileSystem().get());
  assert(cfs);
  const auto& cloud_opts = cfs->GetCloudFileSystemOptions();
  if (std::string(cfs->Name()) != CloudFileSystemImpl::kGcp()) {
    return Status::InvalidArgument("GCS Provider requires GCP Environment");
  }
  if (!cfs->SrcMatchesDest() && cfs->HasSrcBucket() && cfs->HasDestBucket()) {
    if (cloud_opts.src_bucket.GetRegion() !=
        cloud_opts.dest_bucket.GetRegion()) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs->GetLogger(),
          "[gcs] NewGcpFileSystem Buckets %s, %s in two different regions "
          "%s, %s is not supported",
          cloud_opts.src_bucket.GetBucketName().c_str(),
          cloud_opts.dest_bucket.GetBucketName().c_str(),
          cloud_opts.src_bucket.GetRegion().c_str(),
          cloud_opts.dest_bucket.GetRegion().c_str());
      return Status::InvalidArgument("Two different regions not supported");
    }
  }
  gcp::Options gcp_options;
  Status status = GcpCloudOptions::GetClientConfiguration(
      cfs, cloud_opts.src_bucket.GetRegion(), gcp_options);
  if (status.ok()) {
    gcs_client_ = std::make_shared<GCSClientWrapper>(cloud_opts, gcp_options);
    return CloudStorageProviderImpl::PrepareOptions(options);
  }
  return status;
}

IOStatus GcsStorageProvider::DoGetCloudObject(
    const std::string& bucket_name, const std::string& object_path,
    const std::string& destination, uint64_t* remote_size) {
  auto normalized_path = normalize_object_path(object_path);
  auto get = gcs_client_->DownloadFile(bucket_name, normalized_path,
                                       destination, remote_size);
  if (!get.ok()) {
    std::string errmsg = get.message();
    if (IsNotFound(get)) {
      Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
          "[gcs] GetObject %s/%s error %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      return IOStatus::NotFound(std::move(errmsg));
    } else {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
          "[gcs] GetObject %s/%s error %s.", bucket_name.c_str(),
          object_path.c_str(), errmsg.c_str());
      return IOStatus::IOError(std::move(errmsg));
    }
  }
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::DoPutCloudObject(
    const std::string& local_file, const std::string& bucket_name,
    const std::string& object_path, uint64_t file_size,
    const PutObjectOptions& /*options*/) {
  auto normalized_path = normalize_object_path(object_path);
  auto put = gcs_client_->UploadFile(bucket_name, normalized_path, local_file);
  if (!put.ok()) {
    const auto& error = put.status().message();
    std::string errmsg(error.data(), error.size());
    Log(InfoLogLevel::ERROR_LEVEL, cfs_->GetLogger(),
        "[gcs] PutCloudObject %s/%s, size %" PRIu64 ", ERROR %s",
        bucket_name.c_str(), object_path.c_str(), file_size, errmsg.c_str());
    return IOStatus::IOError(local_file, errmsg);
  }

  Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
      "[gcs] PutCloudObject %s/%s, size %" PRIu64 ", OK", bucket_name.c_str(),
      object_path.c_str(), file_size);
  return IOStatus::OK();
}

IOStatus GcsStorageProvider::HeadObject(const std::string& bucket,
                                        const std::string& path,
                                        HeadObjectResult* result) {
  assert(result != nullptr);
  auto object_path = normalize_object_path(path);
  auto head = gcs_client_->HeadObject(bucket, object_path);
  if (!head.ok()) {
    const auto& err_message = head.status().message();
    Slice object_path_slice(object_path.data(), object_path.size());
    if (IsNotFound(head.status())) {
      return IOStatus::NotFound(object_path_slice, err_message.c_str());
    } else {
      return IOStatus::IOError(object_path_slice, err_message.c_str());
    }
  }

  const auto& head_val = head.value();
  if (result->metadata != nullptr) {
    for (const auto& m : head_val.metadata()) {
      (*(result->metadata))[m.first] = m.second;
    }
  }
  if (result->size != nullptr) {
    *(result->size) = head_val.size();
  }
  if (result->modtime != nullptr) {
    int64_t modtime = std::chrono::duration_cast<std::chrono::milliseconds>(
                          head_val.updated().time_since_epoch())
                          .count();
    *(result->modtime) = modtime;
  }
  if (result->etag != nullptr) {
    *(result->etag) =
        std::string(head_val.etag().data(), head_val.etag().length());
  }
  return IOStatus::OK();
}

#endif  // USE_GCS

Status CloudStorageProviderImpl::CreateGcsProvider(
    std::unique_ptr<CloudStorageProvider>* provider) {
#ifndef USE_GCS
  provider->reset();
  return Status::NotSupported(
      "In order to use Google Cloud Storage, make sure you're compiling with "
      "USE_GCS=1");
#else
  provider->reset(new GcsStorageProvider());
  return Status::OK();
#endif
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
