//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifdef ROCKSDB_CLOUD

#include "db/db_impl/db_impl_read_replica.h"

#include <cinttypes>

#include "cloud/cloud_wal_controller.h"
#include "db/arena_wrapped_db_iter.h"
#include "db/merge_context.h"
#include "logging/auto_roll_logger.h"
#include "logging/logging.h"
#include "monitoring/perf_context_imp.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/db.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

DBImplReadReplica::DBImplReadReplica(const DBOptions& db_options,
                                     const std::string& dbname,
                                     std::string local_replica_path,
                                     CloudFileSystem* cfs,
                                     CloudWALController* wal_controller)
    : DBImplSecondary(db_options, dbname, std::move(local_replica_path)),
      cfs_(cfs),
      wal_controller_(wal_controller),
      stop_requested_(false),
      cv_(&mu_) {
  ROCKS_LOG_INFO(immutable_db_options_.info_log,
                 "Opening the db in read replica mode");
  LogFlush(immutable_db_options_.info_log);
}

DBImplReadReplica::~DBImplReadReplica() {
  Status s = Close();
  if (!s.ok()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Error closing DB : %s",
                   s.ToString().c_str());
  }
}

Status DBImplReadReplica::Recover(
    const std::vector<ColumnFamilyDescriptor>& column_families,
    bool /*readonly*/, bool /*error_if_wal_file_exists*/,
    bool /*error_if_data_exists_in_wals*/, bool /*is_retry*/, uint64_t*,
    RecoveryContext* /*recovery_ctx*/, bool* /*can_retry*/) {
  mutex_.AssertHeld();

  Status s;

  // Initial MANIFEST recovery via ReactiveVersionSet
  s = static_cast<ReactiveVersionSet*>(versions_.get())
          ->Recover(column_families, &manifest_reader_, &manifest_reporter_,
                    &manifest_reader_status_);
  if (!s.ok()) {
    if (manifest_reader_status_) {
      manifest_reader_status_->PermitUncheckedError();
    }
    return s;
  }

  max_total_in_memory_state_ = 0;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    const auto& mutable_cf_options = cfd->GetLatestMutableCFOptions();
    max_total_in_memory_state_ += mutable_cf_options.write_buffer_size *
                                  mutable_cf_options.max_write_buffer_number;
  }

  if (s.ok()) {
    default_cf_handle_ = new ColumnFamilyHandleImpl(
        versions_->GetColumnFamilySet()->GetDefault(), this, &mutex_);
    default_cf_internal_stats_ = default_cf_handle_->cfd()->internal_stats();

    // Fetch initial WAL from cloud/Kafka before replaying
    FetchCloudWAL().PermitUncheckedError();

    std::unordered_set<ColumnFamilyData*> cfds_changed;
    JobContext job_context(0);
    s = FindAndRecoverLogFiles(&cfds_changed, &job_context);
    job_context.Clean();
  }

  if (s.IsPathNotFound()) {
    ROCKS_LOG_INFO(immutable_db_options_.info_log,
                   "Read replica tries to read WAL, but WAL file(s) have "
                   "already been purged by primary.");
    s = Status::OK();
  }

  // Start the periodic catch-up thread
  if (s.ok()) {
    catch_up_thread_.reset(
        new port::Thread(&DBImplReadReplica::PeriodicRefresh, this));
  }

  return s;
}

Status DBImplReadReplica::ResyncCloudManifest() {
  assert(cfs_ != nullptr);
  auto io_s = cfs_->PreloadCloudManifest(GetName());
  if (!io_s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Read replica failed to preload CLOUDMANIFEST: %s",
                   io_s.ToString().c_str());
    return static_cast<Status>(io_s);
  }
  io_s = cfs_->LoadCloudManifest(GetName(), /*read_only=*/true);
  if (!io_s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Read replica failed to load CLOUDMANIFEST: %s",
                   io_s.ToString().c_str());
    return static_cast<Status>(io_s);
  }
  return Status::OK();
}

Status DBImplReadReplica::FetchCloudWAL() {
  if (!wal_controller_) {
    return Status::OK();
  }

#ifdef ROCKSDB_CLOUD
  uint32_t sources = immutable_db_options_.read_replica_wal_sources;

  if (sources & DBOptions::kReadReplicaWALCloud) {
    auto io_s = wal_controller_->TailWALFromCloud(GetName());
    if (!io_s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Read replica cloud WAL tail failed: %s",
                     io_s.ToString().c_str());
    }
  }

  if (sources & DBOptions::kReadReplicaWALKafka) {
    auto io_s = wal_controller_->TailWALFromKafka(GetName());
    if (!io_s.ok()) {
      ROCKS_LOG_WARN(immutable_db_options_.info_log,
                     "Read replica Kafka WAL tail failed: %s",
                     io_s.ToString().c_str());
    }
  }
#endif  // ROCKSDB_CLOUD

  return Status::OK();
}

Status DBImplReadReplica::TryCatchUpWithPrimary() {
  assert(versions_.get() != nullptr);
  assert(manifest_reader_.get() != nullptr);
  Status s;

  // 1. Re-sync CLOUDMANIFEST from cloud
  s = ResyncCloudManifest();
  if (!s.ok()) {
    ROCKS_LOG_WARN(immutable_db_options_.info_log,
                   "Read replica CLOUDMANIFEST re-sync failed: %s",
                   s.ToString().c_str());
    return s;
  }

  // 2. Tail MANIFEST and replay WAL
  std::unordered_set<ColumnFamilyData*> cfds_changed;
  JobContext job_context(0, true /*create_superversion*/);
  {
    InstrumentedMutexLock lock_guard(&mutex_);

    // Apply new MANIFEST edits
    s = static_cast_with_check<ReactiveVersionSet>(versions_.get())
            ->ReadAndApply(&mutex_, &manifest_reader_,
                           manifest_reader_status_.get(), &cfds_changed,
                           /*files_to_delete=*/nullptr);

    ROCKS_LOG_INFO(immutable_db_options_.info_log, "Last sequence is %" PRIu64,
                   static_cast<uint64_t>(versions_->LastSequence()));
    for (ColumnFamilyData* cfd : cfds_changed) {
      if (cfd->IsDropped()) {
        ROCKS_LOG_DEBUG(immutable_db_options_.info_log, "[%s] is dropped\n",
                        cfd->GetName().c_str());
        continue;
      }
      VersionStorageInfo::LevelSummaryStorage tmp;
      ROCKS_LOG_DEBUG(immutable_db_options_.info_log,
                      "[%s] Level summary: %s\n", cfd->GetName().c_str(),
                      cfd->current()->storage_info()->LevelSummary(&tmp));
    }

    // 3. Fetch new WAL from cloud/Kafka
    if (s.ok()) {
      FetchCloudWAL().PermitUncheckedError();
    }

    // 4. Replay local WAL files (including newly downloaded ones)
    if (s.ok()) {
      s = FindAndRecoverLogFiles(&cfds_changed, &job_context);
      if (s.IsPathNotFound()) {
        ROCKS_LOG_INFO(
            immutable_db_options_.info_log,
            "Read replica tries to read WAL, but WAL file(s) have already "
            "been purged by primary.");
        s = Status::OK();
      }
    }

    // 5. Install new SuperVersions
    if (s.ok()) {
      for (auto cfd : cfds_changed) {
        cfd->imm()->RemoveOldMemTables(cfd->GetLogNumber(),
                                       &job_context.memtables_to_free);
        auto& sv_context = job_context.superversion_contexts.back();
        cfd->InstallSuperVersion(&sv_context, &mutex_);
        sv_context.NewSuperVersion();
      }
    }
  }
  job_context.Clean();

  // 6. Cleanup obsolete files
  JobContext purge_files_job_context(0);
  {
    InstrumentedMutexLock lock_guard(&mutex_);
    FindObsoleteFiles(&purge_files_job_context, /*force=*/false);
  }
  if (purge_files_job_context.HaveSomethingToDelete()) {
    PurgeObsoleteFiles(purge_files_job_context);
  }
  purge_files_job_context.Clean();
  return s;
}

void DBImplReadReplica::PeriodicRefresh() {
  while (!stop_requested_.load()) {
    MutexLock l(&mu_);
    int64_t wait_until =
        immutable_db_options_.clock->NowMicros() +
        immutable_db_options_.follower_refresh_catchup_period_ms * 1000;
    immutable_db_options_.clock->TimedWait(
        &cv_, std::chrono::microseconds(wait_until));
    if (stop_requested_.load()) {
      break;
    }
    Status s;
    for (uint64_t i = 0;
         i < immutable_db_options_.follower_catchup_retry_count &&
         !stop_requested_.load();
         ++i) {
      s = TryCatchUpWithPrimary();

      if (s.ok()) {
        ROCKS_LOG_INFO(immutable_db_options_.info_log,
                       "Read replica successful catch up on attempt %llu",
                       static_cast<unsigned long long>(i));
        break;
      }
      wait_until =
          immutable_db_options_.clock->NowMicros() +
          immutable_db_options_.follower_catchup_retry_wait_ms * 1000;
      immutable_db_options_.clock->TimedWait(
          &cv_, std::chrono::microseconds(wait_until));
    }
    if (!s.ok()) {
      ROCKS_LOG_INFO(immutable_db_options_.info_log,
                     "Read replica catch up unsuccessful");
    }
  }
}

Status DBImplReadReplica::Close() {
  if (catch_up_thread_) {
    stop_requested_.store(true);
    {
      MutexLock l(&mu_);
      cv_.SignalAll();
    }
    catch_up_thread_->join();
    catch_up_thread_.reset();
  }
  return DBImpl::Close();
}

// ---------------------------------------------------------------------------
// DB::OpenAsReadReplica
// ---------------------------------------------------------------------------

Status DB::OpenAsReadReplica(const Options& options, const std::string& dbname,
                             const std::string& local_replica_path,
                             std::unique_ptr<DB>* dbptr) {
  dbptr->reset();

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.emplace_back(kDefaultColumnFamilyName, cf_options);
  std::vector<ColumnFamilyHandle*> handles;

  Status s = DB::OpenAsReadReplica(db_options, dbname, local_replica_path,
                                   column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }
  return s;
}

Status DB::OpenAsReadReplica(
    const DBOptions& db_options, const std::string& dbname,
    const std::string& local_replica_path,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, std::unique_ptr<DB>* dbptr) {
  dbptr->reset();

  auto* cfs =
      dynamic_cast<CloudFileSystem*>(db_options.env->GetFileSystem().get());
  if (!cfs) {
    return Status::InvalidArgument(
        "OpenAsReadReplica requires a CloudFileSystem");
  }

  // Sanitize local directory and load CLOUDMANIFEST
  auto io_s = cfs->SanitizeLocalDirectory(db_options, dbname, /*read_only=*/true);
  if (!io_s.ok()) return static_cast<Status>(io_s);

  io_s = cfs->PreloadCloudManifest(dbname);
  if (!io_s.ok()) return static_cast<Status>(io_s);

  io_s = cfs->LoadCloudManifest(dbname, /*read_only=*/true);
  if (!io_s.ok()) return static_cast<Status>(io_s);

  // Get the WAL controller from the cloud FS impl (may be nullptr)
  auto* cfs_impl = dynamic_cast<CloudFileSystemImpl*>(cfs);
  CloudWALController* wal_ctrl = nullptr;
  if (cfs_impl) {
    wal_ctrl = cfs_impl->GetWALController();
  }

  // Fetch initial WAL from cloud/Kafka before opening
  if (wal_ctrl) {
    wal_ctrl->RecoverWALFromCloud(dbname).PermitUncheckedError();
    wal_ctrl->RecoverWALFromKafka(dbname).PermitUncheckedError();
  }

  DBOptions tmp_opts(db_options);
  if (nullptr == tmp_opts.info_log) {
    Status s = CreateLoggerFromOptions(dbname, tmp_opts, &tmp_opts.info_log);
    if (!s.ok()) {
      tmp_opts.info_log = nullptr;
      return s;
    }
  }

  handles->clear();
  DBImplReadReplica* impl = new DBImplReadReplica(
      tmp_opts, dbname, local_replica_path, cfs, wal_ctrl);
  impl->versions_.reset(new ReactiveVersionSet(
      dbname, &impl->immutable_db_options_, impl->mutable_db_options_,
      impl->file_options_, impl->table_cache_.get(),
      impl->write_buffer_manager_, &impl->write_controller_, impl->io_tracer_));
  impl->column_family_memtables_.reset(
      new ColumnFamilyMemTablesImpl(impl->versions_->GetColumnFamilySet()));
  impl->wal_in_db_path_ = impl->immutable_db_options_.IsWalDirSameAsDBPath();

  impl->mutex_.Lock();
  Status s = impl->Recover(column_families, /*read_only=*/true,
                            /*error_if_wal_file_exists=*/false,
                            /*error_if_data_exists_in_wals=*/false);
  if (s.ok()) {
    for (const auto& cf : column_families) {
      auto cfd =
          impl->versions_->GetColumnFamilySet()->GetColumnFamily(cf.name);
      if (nullptr == cfd) {
        s = Status::InvalidArgument("Column family not found", cf.name);
        break;
      }
      handles->push_back(new ColumnFamilyHandleImpl(cfd, impl, &impl->mutex_));
    }
  }
  SuperVersionContext sv_context(false /* create_superversion */);
  if (s.ok()) {
    for (auto cfd : *impl->versions_->GetColumnFamilySet()) {
      sv_context.NewSuperVersion();
      cfd->InstallSuperVersion(&sv_context, &impl->mutex_);
    }
  }
  impl->mutex_.Unlock();
  sv_context.Clean();
  if (s.ok()) {
    dbptr->reset(impl);
    for (auto h : *handles) {
      impl->NewThreadStatusCfInfo(
          static_cast_with_check<ColumnFamilyHandleImpl>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      delete h;
    }
    handles->clear();
    delete impl;
  }
  return s;
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_CLOUD
