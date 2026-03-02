// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#pragma once

#include <memory>

#include "rocksdb/cloud/cloud_file_system_impl.h"

#ifdef USE_GCS

namespace ROCKSDB_NAMESPACE {
class GcpFileSystem : public CloudFileSystemImpl {
 public:
  static Status NewGcpFileSystem(const std::shared_ptr<FileSystem>& base_fs,
                                 const CloudFileSystemOptions& cloud_options,
                                 const std::shared_ptr<Logger>& info_log,
                                 CloudFileSystem** cfs);
  static Status NewGcpFileSystem(const std::shared_ptr<FileSystem>& fs,
                                 std::unique_ptr<CloudFileSystem>* cfs);
  virtual ~GcpFileSystem() {}

  static const char* kName() { return kGcp(); }
  const char* Name() const override { return kGcp(); }

  Status PrepareOptions(const ConfigOptions& options) override;

  static constexpr const char* default_region = "us-central1";

 private:
  explicit GcpFileSystem(const std::shared_ptr<FileSystem>& underlying_fs,
                         const CloudFileSystemOptions& cloud_options,
                         const std::shared_ptr<Logger>& info_log = nullptr);
};

class GcpCloudOptions {
 public:
  static Status GetClientConfiguration(CloudFileSystem* fs,
                                       const std::string& region,
                                       google::cloud::Options& options);
};
}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_GCS
