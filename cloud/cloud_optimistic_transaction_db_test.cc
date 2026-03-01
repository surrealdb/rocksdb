// Copyright (c) 2024-present, SurrealDB Inc

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include "rocksdb/cloud/cloud_optimistic_transaction_db.h"

#include <aws/core/Aws.h>

#include <string>
#include <vector>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"

namespace ROCKSDB_NAMESPACE {

class CloudOptimisticTransactionDBTest : public testing::Test {
 public:
  CloudOptimisticTransactionDBTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/cloud_otxn_test-" + test_id_;

    cloud_fs_options_.TEST_Initialize("cloudotxntest.", dbname_);
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    db_ = nullptr;

    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/cloud-otxn-test.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    CleanupBucket();
  }

  ~CloudOptimisticTransactionDBTest() override {
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
    ASSERT_OK(CloudOptimisticTransactionDB::Open(
        options_, dbname_, "", 0, &db_));
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
  std::string dbname_;
  CloudFileSystemOptions cloud_fs_options_;
  CloudOptimisticTransactionDB* db_;
  std::unique_ptr<Env> aenv_;
};

TEST_F(CloudOptimisticTransactionDBTest, OpenAndClose) {
  OpenDB();
  ASSERT_NE(db_, nullptr);

  auto* txn_db = db_->GetTxnDB();
  ASSERT_NE(txn_db, nullptr);
}

TEST_F(CloudOptimisticTransactionDBTest, BasicTransaction) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  WriteOptions wo;
  ReadOptions ro;
  OptimisticTransactionOptions txn_opts;

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

TEST_F(CloudOptimisticTransactionDBTest, TransactionConflict) {
  OpenDB();
  auto* txn_db = db_->GetTxnDB();

  WriteOptions wo;
  OptimisticTransactionOptions txn_opts;
  txn_opts.set_snapshot = true;

  Transaction* txn1 = txn_db->BeginTransaction(wo, txn_opts);
  Transaction* txn2 = txn_db->BeginTransaction(wo, txn_opts);

  ASSERT_OK(txn1->Put("conflict_key", "from_txn1"));
  ASSERT_OK(txn2->Put("conflict_key", "from_txn2"));

  ASSERT_OK(txn1->Commit());
  Status s = txn2->Commit();
  ASSERT_TRUE(s.IsBusy());

  delete txn1;
  delete txn2;

  std::string value;
  ASSERT_OK(txn_db->Get(ReadOptions(), "conflict_key", &value));
  ASSERT_EQ(value, "from_txn1");
}

TEST_F(CloudOptimisticTransactionDBTest, DirectPutGet) {
  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "direct_key", "direct_value"));

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "direct_key", &value));
  ASSERT_EQ(value, "direct_value");
}

TEST_F(CloudOptimisticTransactionDBTest, OpenWithColumnFamilies) {
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

  ASSERT_OK(CloudOptimisticTransactionDB::Open(
      options_, dbname_, cf_descs, "", 0, &handles, &db_));
  ASSERT_EQ(handles.size(), 2);

  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), handles[1], "cf_key", &value));
  ASSERT_EQ(value, "cf_value");

  for (auto* h : handles) delete h;
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
  fprintf(stderr,
          "SKIPPED as CloudOptimisticTransactionDB requires USE_AWS.\n");
  return 0;
}

#endif  // USE_AWS

#else  // ROCKSDB_LITE

#include <stdio.h>
int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as CloudOptimisticTransactionDB is not supported in "
          "ROCKSDB_LITE.\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
