//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class CloudStorageProvider;
class FileSystem;

// Utilities for managing branch ref objects and the branch registry in
// cloud object storage.
class CloudBranchUtil {
 public:
  // Write a ref object at <parent_path>/.refs/<child_dbid>
  static IOStatus WriteRefObject(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& parent_path,
      const BranchInfo& info,
      const std::shared_ptr<FileSystem>& local_fs);

  // Delete the ref object at <parent_path>/.refs/<child_dbid>
  static IOStatus DeleteRefObject(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& parent_path,
      const std::string& child_dbid);

  // List all ref objects under <parent_path>/.refs/
  static IOStatus ListRefObjects(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& parent_path,
      std::vector<BranchInfo>* branches,
      const std::shared_ptr<FileSystem>& local_fs);

  // Write the convenience branch registry at <parent_path>/.branches
  static IOStatus WriteBranchRegistry(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& parent_path,
      const std::vector<BranchInfo>& branches,
      const std::shared_ptr<FileSystem>& local_fs);

  // Read the branch registry from <parent_path>/.branches
  static IOStatus ReadBranchRegistry(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& parent_path,
      std::vector<BranchInfo>* branches,
      const std::shared_ptr<FileSystem>& local_fs);

 private:
  static std::string SerializeBranchInfo(const BranchInfo& info);
  static IOStatus DeserializeBranchInfo(const std::string& json,
                                        BranchInfo* info);
  static std::string SerializeBranchList(
      const std::vector<BranchInfo>& branches);
  static IOStatus DeserializeBranchList(const std::string& json,
                                        std::vector<BranchInfo>* branches);
  static std::string RefObjectPath(const std::string& parent_path,
                                   const std::string& child_dbid);
  static std::string RegistryPath(const std::string& parent_path);

  static IOStatus WriteStringToCloud(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& cloud_path,
      const std::string& content,
      const std::shared_ptr<FileSystem>& local_fs);

  static IOStatus ReadStringFromCloud(
      const std::shared_ptr<CloudStorageProvider>& provider,
      const std::string& bucket, const std::string& cloud_path,
      std::string* content,
      const std::shared_ptr<FileSystem>& local_fs);
};

}  // namespace ROCKSDB_NAMESPACE
