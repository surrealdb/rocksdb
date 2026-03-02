// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/utilities/object_registry.h"

#ifdef USE_GCS

#include "cloud/gcp/gcp_file_system.h"

#include <string>

#include "rocksdb/convenience.h"

namespace ROCKSDB_NAMESPACE {
GcpFileSystem::GcpFileSystem(const std::shared_ptr<FileSystem>& underlying_fs,
                             const CloudFileSystemOptions& cloud_options,
                             const std::shared_ptr<Logger>& info_log)
    : CloudFileSystemImpl(cloud_options, underlying_fs, info_log) {}

Status GcpFileSystem::NewGcpFileSystem(
    const std::shared_ptr<FileSystem>& base_fs,
    const CloudFileSystemOptions& cloud_options,
    const std::shared_ptr<Logger>& info_log, CloudFileSystem** cfs) {
  Status status;
  *cfs = nullptr;
  auto fs = base_fs;
  if (!fs) {
    fs = FileSystem::Default();
  }
  std::unique_ptr<GcpFileSystem> gfs(
      new GcpFileSystem(fs, cloud_options, info_log));
  auto env =
      CloudFileSystemEnv::NewCompositeEnvFromFs(gfs.get(), Env::Default());
  ConfigOptions config_options;
  config_options.env = env.get();
  status = gfs->PrepareOptions(config_options);
  if (status.ok()) {
    *cfs = gfs.release();
  }
  return status;
}

Status GcpFileSystem::NewGcpFileSystem(const std::shared_ptr<FileSystem>& fs,
                                       std::unique_ptr<CloudFileSystem>* cfs) {
  cfs->reset(new GcpFileSystem(fs, CloudFileSystemOptions()));
  return Status::OK();
}

Status GcpFileSystem::PrepareOptions(const ConfigOptions& options) {
  if (cloud_fs_options.src_bucket.GetRegion().empty() ||
      cloud_fs_options.dest_bucket.GetRegion().empty()) {
    std::string region;
    if (!CloudFileSystemOptions::GetNameFromEnvironment(
            "GCP_DEFAULT_REGION", "gcp_default_region", &region)) {
      region = default_region;
    }
    if (cloud_fs_options.src_bucket.GetRegion().empty()) {
      cloud_fs_options.src_bucket.SetRegion(region);
    }
    if (cloud_fs_options.dest_bucket.GetRegion().empty()) {
      cloud_fs_options.dest_bucket.SetRegion(region);
    }
  }
  if (cloud_fs_options.storage_provider == nullptr) {
    Status s = CloudStorageProvider::CreateFromString(
        options, CloudStorageProviderImpl::kGcs(),
        &cloud_fs_options.storage_provider);
    if (!s.ok()) {
      return s;
    }
  }
  return CloudFileSystemImpl::PrepareOptions(options);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_GCS

namespace ROCKSDB_NAMESPACE {
int CloudFileSystemImpl::RegisterGcsObjects(ObjectLibrary& library,
                                            const std::string& /*arg*/) {
  int count = 0;

#ifdef USE_GCS
  library.AddFactory<FileSystem>(
      CloudFileSystemImpl::kGcp(),
      [](const std::string& /*uri*/, std::unique_ptr<FileSystem>* guard,
         std::string* errmsg) {
        std::unique_ptr<CloudFileSystem> cguard;
        Status s =
            GcpFileSystem::NewGcpFileSystem(FileSystem::Default(), &cguard);
        if (s.ok()) {
          guard->reset(cguard.release());
          return guard->get();
        } else {
          *errmsg = s.ToString();
          return static_cast<FileSystem*>(nullptr);
        }
      });
  count++;
#endif  // USE_GCS

  library.AddFactory<CloudStorageProvider>(
      CloudStorageProviderImpl::kGcs(),
      [](const std::string& /*uri*/,
         std::unique_ptr<CloudStorageProvider>* guard, std::string* errmsg) {
        Status s = CloudStorageProviderImpl::CreateGcsProvider(guard);
        if (!s.ok()) {
          *errmsg = s.ToString();
        }
        return guard->get();
      });
  count++;
  return count;
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
