//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//
#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/io_status.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class CloudScheduler;
class CloudStorageProvider;

// Manages async replication of SST/MANIFEST/CLOUDMANIFEST files to secondary
// buckets (potentially in other regions). SSTs are replicated asynchronously
// after primary upload. MANIFEST and CLOUDMANIFEST replication is gated on
// all referenced SSTs being present in every replication bucket.
class CloudReplicationManager {
 public:
  CloudReplicationManager(
      const CloudFileSystemOptions& cloud_opts,
      const std::shared_ptr<Logger>& logger);
  ~CloudReplicationManager();

  // Initialize per-region storage providers for each replication bucket.
  // Must be called after cloud objects are registered.
  Status Initialize(CloudFileSystem* cfs);

  // Schedule async replication of a file to all replication buckets.
  // Called after primary upload succeeds.
  // local_path: path to the local file
  // cloud_name: object name in cloud (e.g. "<object_path>/000042.sst-epoch")
  void ScheduleReplication(const std::string& local_path,
                           const std::string& cloud_name);

  // Block until all pending SST replications have completed.
  IOStatus WaitForAllPending();

  // Upload MANIFEST then CLOUDMANIFEST to every replication bucket.
  // Maintains the same ordering invariant as the primary path.
  IOStatus ReplicateManifestAndCloudManifest(
      const std::string& local_dbname,
      const std::string& epoch,
      const std::string& cookie,
      const std::string& dest_object_path);

  // Delete an object from all replication buckets.
  void ScheduleDeletion(const std::string& cloud_name);

  // Returns true if a file still has pending replications.
  bool HasPendingReplication(const std::string& local_path) const;

  void Stop();

 private:
  struct ReplicationTarget {
    BucketOptions bucket;
    std::shared_ptr<CloudStorageProvider> provider;
  };

  void DoReplicate(const std::string& local_path,
                   const std::string& cloud_name,
                   size_t target_idx);

  CloudFileSystemOptions cloud_opts_;
  std::shared_ptr<Logger> info_log_;
  std::vector<ReplicationTarget> targets_;
  std::shared_ptr<CloudScheduler> scheduler_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  // Maps local file path -> number of outstanding replications
  std::unordered_map<std::string, int> pending_;
  // Maps local file path -> total refcount (for deletion deferral)
  std::unordered_map<std::string, int> file_refcounts_;

  std::atomic<bool> stopped_{false};
};

}  // namespace ROCKSDB_NAMESPACE
