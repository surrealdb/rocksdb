// Copyright (c) 2024-present, SurrealDB Inc

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/cloud/cloud_transaction_db.h"

#include <aws/core/Aws.h>

#include <string>
#include <vector>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class CloudTransactionDBTest : public testing::Test {
 public:
  CloudTransactionDBTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/cloud_txn_test-" + test_id_;

    cloud_fs_options_.TEST_Initialize("cloudtxntest.", dbname_);
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    db_ = nullptr;

    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/cloud-txn-test.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    CleanupBucket();
  }

  ~CloudTransactionDBTest() override {
    CloseDB();
    CleanupBucket();
  }

  void CleanupBucket() {
    if (cloud_fs_options_.src_bucket.GetBucketName().empty()) return;
    CloudFileSystem* cfs;
    Status st = CloudFileSystemEnv::NewAwsFileSystem(
        base_env_->GetFileSystem(), cloud_fs_options_, options_.info_log, &cfs);
    if (st.ok()) {
      cfs->GetStorageProvider()->EmptyBucket(cfs->GetSrcBucketName(), dbname_);
      delete cfs;
    }
  }

  void CreateCloudEnv() {
    CloudFileSystem* cfs;
    ASSERT_OK(CloudFileSystemEnv::NewAwsFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &cfs));
    std::shared_ptr<FileSystem> fs(cfs);
    aenv_ = CloudFileSystemEnv::NewCompositeEnv(base_env_, std::move(fs));
  }

  void OpenDB() {
    ASSERT_TRUE(cloud_fs_options_.credentials.HasValid().ok());
    CreateCloudEnv();
    options_.env = aenv_.get();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    ASSERT_EQ(db_, nullptr);
    ASSERT_OK(CloudTransactionDB::Open(options_, txn_db_options_, dbname_,
                                       "", 0, &db_));
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());
      delete db_;
      db_ = nullptr;
    }
    aenv_.reset();
  }

 protected:
  std::string test_id_;
  Env* base_env_;
  Options options_;
  TransactionDBOptions txn_db_options_;
  std::string dbname_;
  CloudFileSystemOptions cloud_fs_options_;
  CloudTransactionDB* db_;
  std::unique_ptr<Env> aenv_;
};

TEST_F(CloudTransactionDBTest, OpenAndClose) {
  OpenDB();
  ASSERT_NE(db_, nullptr);

  auto* txn_db = db_->GetTxnDB();
  ASSERT_NE(txn_db, nullptr);
}

TEST_F(CloudTransactionDBTest, BasicTransaction) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  WriteOptions wo;
  ReadOptions ro;
  TransactionOptions txn_opts;

  Transaction* txn = txn_db->BeginTransaction(wo, txn_opts);
  ASSERT_NE(txn, nullptr);

  ASSERT_OK(txn->Put("key1", "value1"));
  ASSERT_OK(txn->Put("key2", "value2"));
  ASSERT_OK(txn->Commit());
  delete txn;

  std::string value;
  ASSERT_OK(txn_db->Get(ro, "key1", &value));
  ASSERT_EQ(value, "value1");
  ASSERT_OK(txn_db->Get(ro, "key2", &value));
  ASSERT_EQ(value, "value2");
}

TEST_F(CloudTransactionDBTest, PessimisticLocking) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  WriteOptions wo;
  TransactionOptions txn_opts;
  txn_opts.lock_timeout = 100;  // 100ms

  Transaction* txn1 = txn_db->BeginTransaction(wo, txn_opts);
  Transaction* txn2 = txn_db->BeginTransaction(wo, txn_opts);

  ASSERT_OK(txn1->Put("lock_key", "from_txn1"));

  // txn2 should time out trying to write the same key
  Status s = txn2->Put("lock_key", "from_txn2");
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn1->Commit());
  delete txn1;
  delete txn2;

  std::string value;
  ASSERT_OK(txn_db->Get(ReadOptions(), "lock_key", &value));
  ASSERT_EQ(value, "from_txn1");
}

TEST_F(CloudTransactionDBTest, GetForUpdate) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  ASSERT_OK(txn_db->Put(WriteOptions(), "gfu_key", "initial"));

  WriteOptions wo;
  TransactionOptions txn_opts;
  txn_opts.lock_timeout = 100;

  Transaction* txn = txn_db->BeginTransaction(wo, txn_opts);
  std::string value;
  ASSERT_OK(txn->GetForUpdate(ReadOptions(), "gfu_key", &value));
  ASSERT_EQ(value, "initial");

  // Another transaction should fail to write this key
  Transaction* txn2 = txn_db->BeginTransaction(wo, txn_opts);
  Status s = txn2->Put("gfu_key", "conflict");
  ASSERT_TRUE(s.IsTimedOut());

  ASSERT_OK(txn->Put("gfu_key", "updated"));
  ASSERT_OK(txn->Commit());
  delete txn;
  delete txn2;

  ASSERT_OK(txn_db->Get(ReadOptions(), "gfu_key", &value));
  ASSERT_EQ(value, "updated");
}

TEST_F(CloudTransactionDBTest, DirectPutGet) {
  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "direct_key", "direct_value"));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "direct_key", &value));
  ASSERT_EQ(value, "direct_value");
}

TEST_F(CloudTransactionDBTest, OpenWithColumnFamilies) {
  {
    OpenDB();
    ColumnFamilyHandle* cf_handle;
    ASSERT_OK(db_->CreateColumnFamily(options_, "test_cf", &cf_handle));
    ASSERT_OK(db_->Put(WriteOptions(), cf_handle, "cf_key", "cf_value"));
    db_->Flush(FlushOptions());
    delete cf_handle;
    CloseDB();
  }

  CreateCloudEnv();
  options_.env = aenv_.get();
  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, options_);
  cf_descs.emplace_back("test_cf", options_);
  std::vector<ColumnFamilyHandle*> handles;

  ASSERT_OK(CloudTransactionDB::Open(options_, txn_db_options_, dbname_,
                                     cf_descs, "", 0, &handles, &db_));
  ASSERT_EQ(handles.size(), 2);

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), handles[1], "cf_key", &value));
  ASSERT_EQ(value, "cf_value");

  for (auto* h : handles) delete h;
}

TEST_F(CloudTransactionDBTest, TransactionRollback) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  WriteOptions wo;
  TransactionOptions txn_opts;

  Transaction* txn = txn_db->BeginTransaction(wo, txn_opts);
  ASSERT_OK(txn->Put("rollback_key", "should_not_exist"));
  ASSERT_OK(txn->Rollback());
  delete txn;

  std::string value;
  Status s = txn_db->Get(ReadOptions(), "rollback_key", &value);
  ASSERT_TRUE(s.IsNotFound());
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  Aws::InitAPI(Aws::SDKOptions());
  auto r = RUN_ALL_TESTS();
  Aws::ShutdownAPI(Aws::SDKOptions());
  return r;
}

#else  // USE_AWS

#include <stdio.h>
int main(int, char**) {
  fprintf(stderr, "SKIPPED as CloudTransactionDB requires USE_AWS.\n");
  return 0;
}

#endif  // USE_AWS

#else  // ROCKSDB_LITE

#include <stdio.h>
int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as CloudTransactionDB is not supported in ROCKSDB_LITE.\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
