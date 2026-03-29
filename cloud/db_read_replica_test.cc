// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#ifdef USE_AWS

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <filesystem>

#include "cloud/cloud_manifest.h"
#include "cloud/cloud_wal_controller.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "db/db_impl/db_impl.h"
#include "db/db_test_util.h"
#include "file/filename.h"
#include "logging/logging.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {

class ReadReplicaTest : public testing::Test {
 public:
  ReadReplicaTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/db_read_replica-" + test_id_;
    replica_path_ = test::TmpDir() + "/replica-" + test_id_;

    cloud_fs_options_.TEST_Initialize("readreplica.", dbname_);
    cloud_fs_options_.use_aws_transfer_manager = true;
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;

    DestroyDir(dbname_);
    DestroyDir(replica_path_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->CreateDirIfMissing(replica_path_);

    base_env_->NewLogger(test::TmpDir(base_env_) + "/read-replica-test.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);
  }

  ~ReadReplicaTest() override {
    CloseDB();
    CloseReplica();
    DestroyDir(dbname_);
    DestroyDir(replica_path_);
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc __attribute__((unused)) = system(cmd.c_str());
  }

  Status OpenPrimary() {
    CloudFileSystemEnv::RegisterCloudObjects();
    std::unique_ptr<CloudFileSystem> cfs;
    Status s = CloudFileSystemEnv::CreateFromString(
        ConfigOptions(), CloudFileSystem::kAws(), cloud_fs_options_, &cfs);
    if (!s.ok()) return s;

    cfs->SetLogger(options_.info_log);
    cloud_env_.reset(
        CloudFileSystemEnv::NewCompositeEnvFromFs(cfs.release(), base_env_));

    options_.env = cloud_env_.get();
    DBCloud* dbcloud = nullptr;
    s = DBCloud::Open(options_, dbname_, "", 0, &dbcloud);
    if (s.ok()) {
      db_.reset(dbcloud);
    }
    return s;
  }

  Status OpenReplica() {
    CloudFileSystemEnv::RegisterCloudObjects();
    std::unique_ptr<CloudFileSystem> cfs;
    Status s = CloudFileSystemEnv::CreateFromString(
        ConfigOptions(), CloudFileSystem::kAws(), cloud_fs_options_, &cfs);
    if (!s.ok()) return s;

    cfs->SetLogger(options_.info_log);
    replica_env_.reset(
        CloudFileSystemEnv::NewCompositeEnvFromFs(cfs.release(), base_env_));

    Options replica_opts = options_;
    replica_opts.env = replica_env_.get();
    replica_opts.create_if_missing = false;
    replica_opts.read_replica_wal_sources =
        DBOptions::kReadReplicaWALLocal | DBOptions::kReadReplicaWALCloud;
    replica_opts.follower_refresh_catchup_period_ms = 500;

    std::unique_ptr<DB> db;
    s = DB::OpenAsReadReplica(replica_opts, dbname_, replica_path_, &db);
    if (s.ok()) {
      replica_ = std::move(db);
    }
    return s;
  }

  void CloseDB() { db_.reset(); }
  void CloseReplica() { replica_.reset(); }

 protected:
  std::string test_id_;
  Env* base_env_;
  std::string dbname_;
  std::string replica_path_;
  CloudFileSystemOptions cloud_fs_options_;
  Options options_;
  std::unique_ptr<Env> cloud_env_;
  std::unique_ptr<Env> replica_env_;
  std::unique_ptr<DBCloud> db_;
  std::unique_ptr<DB> replica_;
};

TEST_F(ReadReplicaTest, OpenAndRead) {
  ASSERT_OK(OpenPrimary());

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  CloseDB();

  ASSERT_OK(OpenReplica());

  std::string val;
  ASSERT_OK(replica_->Get(ReadOptions(), "key1", &val));
  ASSERT_EQ(val, "value1");
  ASSERT_OK(replica_->Get(ReadOptions(), "key2", &val));
  ASSERT_EQ(val, "value2");
}

TEST_F(ReadReplicaTest, WritesRejected) {
  ASSERT_OK(OpenPrimary());
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  CloseDB();

  ASSERT_OK(OpenReplica());

  Status s = replica_->Put(WriteOptions(), "key3", "value3");
  ASSERT_TRUE(s.IsNotSupported());

  s = replica_->Delete(WriteOptions(), "key1");
  ASSERT_TRUE(s.IsNotSupported());

  WriteBatch batch;
  batch.Put("key4", "value4");
  s = replica_->Write(WriteOptions(), &batch);
  ASSERT_TRUE(s.IsNotSupported());
}

TEST_F(ReadReplicaTest, CatchUpWithPrimary) {
  ASSERT_OK(OpenPrimary());
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  // Savepoint to ensure SSTs are in cloud
  ASSERT_OK(db_->Savepoint());

  ASSERT_OK(OpenReplica());

  std::string val;
  ASSERT_OK(replica_->Get(ReadOptions(), "key1", &val));
  ASSERT_EQ(val, "value1");

  // Write more data on primary
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Savepoint());

  // Catch up on replica
  ASSERT_OK(replica_->TryCatchUpWithPrimary());

  ASSERT_OK(replica_->Get(ReadOptions(), "key2", &val));
  ASSERT_EQ(val, "value2");
}

TEST_F(ReadReplicaTest, ManifestRotation) {
  ASSERT_OK(OpenPrimary());

  for (int i = 0; i < 10; i++) {
    ASSERT_OK(
        db_->Put(WriteOptions(), "key" + std::to_string(i), "val" + std::to_string(i)));
    ASSERT_OK(db_->Flush(FlushOptions()));
  }
  ASSERT_OK(db_->Savepoint());
  CloseDB();

  ASSERT_OK(OpenReplica());
  std::string val;
  ASSERT_OK(replica_->Get(ReadOptions(), "key9", &val));
  ASSERT_EQ(val, "val9");
}

TEST_F(ReadReplicaTest, ColumnFamilies) {
  ASSERT_OK(OpenPrimary());
  ColumnFamilyHandle* cfh = nullptr;
  ASSERT_OK(
      db_->CreateColumnFamily(ColumnFamilyOptions(), "test_cf", &cfh));
  ASSERT_OK(db_->Put(WriteOptions(), cfh, "cf_key", "cf_value"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  ASSERT_OK(db_->Savepoint());
  delete cfh;
  CloseDB();

  // Open replica with column families
  CloudFileSystemEnv::RegisterCloudObjects();
  std::unique_ptr<CloudFileSystem> cfs;
  ASSERT_OK(CloudFileSystemEnv::CreateFromString(
      ConfigOptions(), CloudFileSystem::kAws(), cloud_fs_options_, &cfs));
  cfs->SetLogger(options_.info_log);
  replica_env_.reset(
      CloudFileSystemEnv::NewCompositeEnvFromFs(cfs.release(), base_env_));

  DBOptions db_opts(options_);
  db_opts.env = replica_env_.get();
  db_opts.create_if_missing = false;
  db_opts.read_replica_wal_sources = DBOptions::kReadReplicaWALLocal;

  std::vector<ColumnFamilyDescriptor> cf_descs;
  cf_descs.emplace_back(kDefaultColumnFamilyName, ColumnFamilyOptions());
  cf_descs.emplace_back("test_cf", ColumnFamilyOptions());

  std::vector<ColumnFamilyHandle*> handles;
  std::unique_ptr<DB> db;
  ASSERT_OK(DB::OpenAsReadReplica(db_opts, dbname_, replica_path_, cf_descs,
                                  &handles, &db));
  ASSERT_EQ(handles.size(), 2u);

  std::string val;
  ASSERT_OK(db->Get(ReadOptions(), handles[1], "cf_key", &val));
  ASSERT_EQ(val, "cf_value");

  for (auto h : handles) delete h;
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_AWS

int main() { return 0; }

#endif  // USE_AWS

#endif  // ROCKSDB_LITE
