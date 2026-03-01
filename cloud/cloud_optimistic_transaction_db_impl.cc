// Copyright (c) 2024-present, SurrealDB Inc

#include "cloud/cloud_optimistic_transaction_db_impl.h"

#include <memory>
#include <string>
#include <vector>

#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/cast_util.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"

namespace ROCKSDB_NAMESPACE {

CloudOptimisticTransactionDBImpl::CloudOptimisticTransactionDBImpl(
    DBCloudImpl* db_cloud, OptimisticTransactionDBImpl* txn_db)
    : CloudOptimisticTransactionDB(
          // No-op deleter: db_cloud_ owns the base DB, not this shared_ptr.
          std::shared_ptr<DB>(db_cloud->GetBaseDB(), [](DB*) {})),
      db_cloud_(db_cloud),
      txn_db_(txn_db) {}

CloudOptimisticTransactionDBImpl::~CloudOptimisticTransactionDBImpl() {
  delete txn_db_;
  delete db_cloud_;
}

Status CloudOptimisticTransactionDBImpl::Savepoint() {
  return db_cloud_->Savepoint();
}

Status CloudOptimisticTransactionDBImpl::CheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  return db_cloud_->CheckpointToCloud(destination, options);
}

Status CloudOptimisticTransactionDB::Open(
    const Options& options, const std::string& name,
    const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb,
    CloudOptimisticTransactionDB** dbptr,
    const OptimisticTransactionDBOptions& occ_options) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  Status s = CloudOptimisticTransactionDB::Open(
      options, name, column_families, persistent_cache_path,
      persistent_cache_size_gb, &handles, dbptr, occ_options);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }

  return s;
}

Status CloudOptimisticTransactionDB::Open(
    const Options& opts, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb,
    std::vector<ColumnFamilyHandle*>* handles,
    CloudOptimisticTransactionDB** dbptr,
    const OptimisticTransactionDBOptions& occ_options) {
  // Enable MemTable History if not already enabled, matching the pattern
  // from OptimisticTransactionDB::Open.
  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  for (auto& column_family : column_families_copy) {
    ColumnFamilyOptions* cf_opts = &column_family.options;
    if (cf_opts->max_write_buffer_size_to_maintain == 0) {
      cf_opts->max_write_buffer_size_to_maintain = -1;
    }
  }

  DBCloud* db = nullptr;
  Status st = DBCloud::Open(opts, dbname, column_families_copy,
                            persistent_cache_path, persistent_cache_size_gb,
                            handles, &db, false);
  if (!st.ok()) {
    return st;
  }

  auto* db_cloud = static_cast_with_check<DBCloudImpl>(db);
  // Non-owning shared_ptr: db_cloud owns the base DB.
  auto non_owning_db =
      std::shared_ptr<DB>(db_cloud->GetBaseDB(), [](DB*) {});
  auto* txn_db = new OptimisticTransactionDBImpl(
      std::move(non_owning_db), occ_options);

  *dbptr = new CloudOptimisticTransactionDBImpl(db_cloud, txn_db);

  std::string dbid;
  db->GetDbIdentity(dbid);
  Log(InfoLogLevel::INFO_LEVEL, db->GetOptions().info_log,
      "Opened Cloud OptimisticTransactionDB with local dir %s dbid %s. %s",
      db->GetName().c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
