//  Copyright (c) 2017-present, Rockset

#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/stackable_db.h"

namespace ROCKSDB_NAMESPACE {

// Captures the exact position in the CloudManifest where a branch diverges.
// This is a metadata-only snapshot — no SSTs are copied.
struct ForkPoint {
  std::string epoch;
  uint64_t file_number;
  std::string cloud_manifest_cookie;
};

struct CreateBranchOptions {
  // If true, flush memtable to SSTs before branching. Guarantees branch
  // includes all data, but causes a brief write stall.
  bool flush_memtable = false;
  // When true and flush_memtable is false, trigger a synchronous WAL upload
  // and server-side-copy the parent's WAL files to the child's path.
  // Requires background_wal_sync_to_cloud on the parent.
  // Ignored when flush_memtable is true.
  bool include_wal = true;
};

struct BranchInfo {
  std::string dbid;
  std::string object_path;
  std::string bucket_name;
  uint64_t fork_file_number;
  std::string fork_epoch;
  uint64_t created_at;
};

//
// Database with Cloud support.
//
// Important: The caller is responsible for ensuring that only one database at
// a time is running with the same cloud destination bucket and path. Running
// two databases concurrently with the same destination path will lead to
// corruption if it lasts for more than couple of minutes.
class DBCloud : public StackableDB {
 public:
  // This API is to open a DB when key-values are to be made durable by
  // backing up database state into a cloud-storage system like S3.
  // All kv updates are persisted in cloud-storage.
  // options.env->GetFileSystem() is an object of type
  // ROCKSDB_NAMESPACE::CloudFileSystem and the cloud buckets are specified
  // there.
  static Status Open(const Options& options, const std::string& name,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only = false);

  // This is for advanced users who can comprehend column families.
  // If you want sst files from S3 to be cached in local SSD/disk, then
  // persistent_cache_path should be the pathname of the local
  // cache storage.
  // TODO(igor/dhruba) The first argument here should be DBOptions, just like in
  // DB class.
  static Status Open(const Options& options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only = false);

  // Synchronously copy all relevant files (if any) from source cloud storage to
  // destination cloud storage.
  virtual Status Savepoint() = 0;

  // Synchronously copy all local files to the cloud destination given by
  // 'destination' parameter.
  // Important: This will overwrite the database in 'destination', if any.
  // This feature should be considered experimental.
  virtual Status CheckpointToCloud(const BucketOptions& destination,
                                   const CheckpointToCloudOptions& options) = 0;

  // Capture a lightweight fork point: the current epoch, next file number,
  // and CLOUDMANIFEST cookie. The cloud API can store this externally and
  // use it to create a zero-copy branch that shares the parent's SSTs
  // for file numbers below the fork point.
  virtual Status CaptureForkPoint(ForkPoint* result) = 0;

  // Create a zero-copy branch of this database at the given destination.
  // The branch shares the parent's SST files (via fallback_buckets) and
  // optionally includes WAL files for complete data coverage.
  // A ref object is written to the parent's path to protect referenced SSTs
  // from purger deletion.
  virtual Status CreateBranch(const BucketOptions& destination,
                              const CreateBranchOptions& options,
                              BranchInfo* result) = 0;

  // Detach this database from its parent branch. Server-side-copies all
  // referenced parent SSTs into this database's own path, removes the ref
  // from the parent, and clears fallback_buckets.
  virtual Status DetachBranch() = 0;

  // List all child branches of this database.
  virtual Status ListBranches(std::vector<BranchInfo>* branches) = 0;

  // ListColumnFamilies will open the DB specified by argument name
  // and return the list of all column families in that DB
  // through column_families argument. The ordering of
  // column families in column_families is unspecified.
  static Status ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families);

  virtual ~DBCloud() {}

 protected:
  explicit DBCloud(DB* db) : StackableDB(db) {}
  explicit DBCloud(std::shared_ptr<DB> db) : StackableDB(std::move(db)) {}
};

}  // namespace ROCKSDB_NAMESPACE
