// Copyright (c) 2024-present, SurrealDB Inc

#include "cloud/cloud_transaction_db_impl.h"

#include <memory>
#include <string>
#include <vector>

#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/transaction_db.h"
#include "util/cast_util.h"

namespace ROCKSDB_NAMESPACE {

CloudTransactionDBImpl::CloudTransactionDBImpl(DBCloudImpl* db_cloud,
                                               TransactionDB* txn_db)
    : CloudTransactionDB(
          // No-op deleter: txn_db_ owns db_cloud_ which owns the base DB.
          std::shared_ptr<DB>(db_cloud->GetBaseDB(), [](DB*) {})),
      db_cloud_(db_cloud),
      txn_db_(txn_db) {}

CloudTransactionDBImpl::~CloudTransactionDBImpl() {
  // txn_db_ owns db_cloud_ through the StackableDB chain.
  // Deleting txn_db_ cascades to delete db_cloud_ and the base DB.
  delete txn_db_;
}

Status CloudTransactionDBImpl::Savepoint() {
  return db_cloud_->Savepoint();
}

Status CloudTransactionDBImpl::CheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  return db_cloud_->CheckpointToCloud(destination, options);
}

Status CloudTransactionDBImpl::CaptureForkPoint(ForkPoint* result) {
  return db_cloud_->CaptureForkPoint(result);
}

Status CloudTransactionDB::Open(
    const Options& options, const TransactionDBOptions& txn_db_options,
    const std::string& name, const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb, CloudTransactionDB** dbptr) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  Status s = CloudTransactionDB::Open(
      options, txn_db_options, name, column_families, persistent_cache_path,
      persistent_cache_size_gb, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    delete handles[0];
  }

  return s;
}

Status CloudTransactionDB::Open(
    const Options& opts, const TransactionDBOptions& txn_db_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb,
    std::vector<ColumnFamilyHandle*>* handles, CloudTransactionDB** dbptr) {
  // PrepareWrap enables memtable history, 2PC, and temporarily disables
  // auto-compaction to prevent races during open.
  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  DBOptions db_options(opts);
  std::vector<size_t> compaction_enabled_cf_indices;
  TransactionDB::PrepareWrap(&db_options, &column_families_copy,
                             &compaction_enabled_cf_indices);

  Options options_copy(db_options, opts);
  DBCloud* db = nullptr;
  Status st = DBCloud::Open(options_copy, dbname, column_families_copy,
                            persistent_cache_path, persistent_cache_size_gb,
                            handles, &db, false);
  if (!st.ok()) {
    return st;
  }

  auto* db_cloud = static_cast_with_check<DBCloudImpl>(db);

  // WrapStackableDB takes ownership of db_cloud. The resulting TransactionDB
  // will own db_cloud through its StackableDB chain.
  TransactionDB* txn_db = nullptr;
  st = TransactionDB::WrapStackableDB(db_cloud, txn_db_options,
                                      compaction_enabled_cf_indices, *handles,
                                      &txn_db);
  if (!st.ok()) {
    // On failure, WrapStackableDB may have already deleted db_cloud.
    return st;
  }

  *dbptr = new CloudTransactionDBImpl(db_cloud, txn_db);

  std::string dbid;
  db_cloud->GetDbIdentity(dbid);
  Log(InfoLogLevel::INFO_LEVEL, db_cloud->GetOptions().info_log,
      "Opened Cloud TransactionDB with local dir %s dbid %s. %s",
      db_cloud->GetName().c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
