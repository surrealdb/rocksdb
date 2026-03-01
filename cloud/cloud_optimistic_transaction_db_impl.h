// Copyright (c) 2024-present, SurrealDB Inc

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_optimistic_transaction_db.h"
#include "rocksdb/db.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"

namespace ROCKSDB_NAMESPACE {

// All writes to this OptimisticTransactionDB are configured to be persisted
// in cloud storage.
class CloudOptimisticTransactionDBImpl
    : public CloudOptimisticTransactionDB {
 public:
  CloudOptimisticTransactionDBImpl(DBCloudImpl* db_cloud,
                                   OptimisticTransactionDBImpl* txn_db);

  ~CloudOptimisticTransactionDBImpl() override;

  OptimisticTransactionDB* GetTxnDB() override { return txn_db_; }

  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

 private:
  DBCloudImpl* db_cloud_;
  OptimisticTransactionDBImpl* txn_db_;
};

}  // namespace ROCKSDB_NAMESPACE
