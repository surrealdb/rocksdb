//  Copyright (c) 2024-present, SurrealDB Inc

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/utilities/transaction_db.h"

namespace ROCKSDB_NAMESPACE {

// TransactionDB (pessimistic) with Cloud support.
//
// Important: The caller is responsible for ensuring that only one database at
// a time is running with the same cloud destination bucket and path. Running
// two databases concurrently with the same destination path will lead to
// corruption if it lasts for more than couple of minutes.
class CloudTransactionDB : public DBCloud {
 public:
  static Status Open(const Options& options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& name,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     CloudTransactionDB** dbptr);

  static Status Open(const Options& options,
                     const TransactionDBOptions& txn_db_options,
                     const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles,
                     CloudTransactionDB** dbptr);

  virtual TransactionDB* GetTxnDB() = 0;

  virtual ~CloudTransactionDB() {}

 protected:
  explicit CloudTransactionDB(std::shared_ptr<DB> db)
      : DBCloud(std::move(db)) {}
};

}  // namespace ROCKSDB_NAMESPACE
