// Copyright (c) 2024-present, SurrealDB Inc

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "cloud/db_cloud_impl.h"
#include "rocksdb/cloud/cloud_transaction_db.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {

// All writes to this TransactionDB are configured to be persisted
// in cloud storage.
class CloudTransactionDBImpl : public CloudTransactionDB {
 public:
  // db_cloud is not owned by this class; it is owned by txn_db through the
  // StackableDB chain. Deleting txn_db_ cascades to delete db_cloud_.
  CloudTransactionDBImpl(DBCloudImpl* db_cloud, TransactionDB* txn_db);

  ~CloudTransactionDBImpl() override;

  TransactionDB* GetTxnDB() override { return txn_db_; }

  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

 private:
  DBCloudImpl* db_cloud_;
  TransactionDB* txn_db_;
};

}  // namespace ROCKSDB_NAMESPACE
