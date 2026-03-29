//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef ROCKSDB_CLOUD

#include <atomic>
#include <string>
#include <vector>

#include "db/db_impl/db_impl_secondary.h"
#include "port/port.h"

namespace ROCKSDB_NAMESPACE {

class CloudFileSystem;
class CloudWALController;

// A cloud-aware read replica that continuously re-syncs from
// MANIFEST/CLOUDMANIFEST and replays WAL from local storage, cloud object
// storage, or Kafka -- without closing and reopening the database.
//
// Extends DBImplSecondary (which provides local MANIFEST tailing via
// ReactiveVersionSet and WAL replay via FindAndRecoverLogFiles). This class
// adds:
//   - CLOUDMANIFEST re-sync before each catch-up cycle.
//   - WAL fetching from cloud (S3/GCS) and/or Kafka before local replay.
//   - A background thread that periodically calls TryCatchUpWithLeader().
class DBImplReadReplica : public DBImplSecondary {
 public:
  DBImplReadReplica(const DBOptions& db_options, const std::string& dbname,
                    std::string local_replica_path, CloudFileSystem* cfs,
                    CloudWALController* wal_controller);
  ~DBImplReadReplica() override;

  Status Close() override;

  // Re-sync CLOUDMANIFEST, tail MANIFEST, fetch + replay WAL from all
  // configured sources, and install new SuperVersions.
  Status TryCatchUpWithPrimary() override;

 protected:
  bool OwnTablesAndLogs() const override { return false; }

  Status Recover(const std::vector<ColumnFamilyDescriptor>& column_families,
                 bool readonly, bool error_if_wal_file_exists,
                 bool error_if_data_exists_in_wals, bool is_retry = false,
                 uint64_t* = nullptr, RecoveryContext* recovery_ctx = nullptr,
                 bool* can_retry = nullptr) override;

 private:
  friend class DB;

  DBImplReadReplica(const DBImplReadReplica&) = delete;
  void operator=(const DBImplReadReplica&) = delete;

  Status ResyncCloudManifest();
  Status FetchCloudWAL();
  void PeriodicRefresh();

  CloudFileSystem* cfs_;
  CloudWALController* wal_controller_;  // may be nullptr
  std::string local_replica_path_;

  std::unique_ptr<port::Thread> catch_up_thread_;
  std::atomic<bool> stop_requested_;
  port::Mutex mu_;
  port::CondVar cv_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_CLOUD
