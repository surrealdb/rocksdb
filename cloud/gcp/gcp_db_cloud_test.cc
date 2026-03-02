// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#ifdef USE_GCS

#include "rocksdb/cloud/db_cloud.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <filesystem>

#include "cloud/cloud_manifest.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "db/db_impl/db_impl.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider_impl.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/random.h"
#include "util/string_util.h"
#ifndef OS_WIN
#include <unistd.h>
#endif

namespace ROCKSDB_NAMESPACE {

namespace {
const FileOptions kFileOptions;
const IOOptions kIOOptions;
IODebugContext* const kDbg = nullptr;
}  // namespace

class GcpCloudTest : public testing::Test {
 public:
  GcpCloudTest() {
    Random64 rng(time(nullptr));
    test_id_ = std::to_string(rng.Next());
    fprintf(stderr, "Test ID: %s\n", test_id_.c_str());

    base_env_ = Env::Default();
    dbname_ = test::TmpDir() + "/gcp_db_cloud-" + test_id_;
    clone_dir_ = test::TmpDir() + "/gcp_ctest-" + test_id_;
    cloud_fs_options_.TEST_Initialize("gcptest.", dbname_);
    cloud_fs_options_.cloud_file_deletion_delay = std::chrono::seconds(0);

    options_.create_if_missing = true;
    options_.stats_dump_period_sec = 0;
    options_.stats_persist_period_sec = 0;
    persistent_cache_path_ = "";
    persistent_cache_size_gb_ = 0;
    db_ = nullptr;

    DestroyDir(dbname_);
    base_env_->CreateDirIfMissing(dbname_);
    base_env_->NewLogger(test::TmpDir(base_env_) + "/rocksdb-gcp-cloud.log",
                         &options_.info_log);
    options_.info_log->SetInfoLogLevel(InfoLogLevel::DEBUG_LEVEL);

    Cleanup();
  }

  void Cleanup() {
    ASSERT_TRUE(!cloud_env_);

    CloudFileSystem* cfs;
    ASSERT_OK(CloudFileSystemEnv::NewGcpFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &cfs));
    ASSERT_NE(cfs, nullptr);
    auto st = cfs->GetStorageProvider()->EmptyBucket(cfs->GetSrcBucketName(),
                                                     dbname_);
    delete cfs;
    ASSERT_TRUE(st.ok() || st.IsNotFound());

    DestroyDir(clone_dir_);
    ASSERT_OK(base_env_->CreateDir(clone_dir_));
  }

  void DestroyDir(const std::string& dir) {
    std::string cmd = "rm -rf " + dir;
    int rc = system(cmd.c_str());
    ASSERT_EQ(rc, 0);
  }

  virtual ~GcpCloudTest() {
    CloseDB();
    cloud_env_.reset();
    if (!cloud_fs_options_.src_bucket.GetBucketName().empty()) {
      CloudFileSystem* cfs;
      Status st = CloudFileSystemEnv::NewGcpFileSystem(
          base_env_->GetFileSystem(), cloud_fs_options_, options_.info_log,
          &cfs);
      if (st.ok()) {
        cfs->GetStorageProvider()->EmptyBucket(cfs->GetSrcBucketName(),
                                               dbname_);
        delete cfs;
      }
    }
  }

  void CreateCloudEnv() {
    CloudFileSystem* cfs;
    ASSERT_OK(CloudFileSystemEnv::NewGcpFileSystem(base_env_->GetFileSystem(),
                                                   cloud_fs_options_,
                                                   options_.info_log, &cfs));
    std::shared_ptr<FileSystem> fs(cfs);
    cloud_env_ = CloudFileSystemEnv::NewCompositeEnv(base_env_, std::move(fs));
  }

  void OpenDB() {
    std::vector<ColumnFamilyHandle*> handles;
    OpenDB(&handles);
    ASSERT_TRUE(handles.size() > 0);
    delete handles[0];
  }

  void OpenDB(std::vector<ColumnFamilyHandle*>* handles) {
    OpenWithColumnFamilies({kDefaultColumnFamilyName}, handles);
  }

  void OpenWithColumnFamilies(const std::vector<std::string>& cfs,
                              std::vector<ColumnFamilyHandle*>* handles) {
    CreateCloudEnv();
    options_.env = cloud_env_.get();

    ASSERT_TRUE(db_ == nullptr);
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.emplace_back(cfs[i], options_);
    }
    ASSERT_OK(DBCloud::Open(options_, dbname_, column_families,
                            persistent_cache_path_, persistent_cache_size_gb_,
                            handles, &db_));
    ASSERT_OK(db_->GetDbIdentity(dbid_));
  }

  Status checkOpen() {
    CreateCloudEnv();
    options_.env = cloud_env_.get();
    return DBCloud::Open(options_, dbname_, persistent_cache_path_,
                         persistent_cache_size_gb_, &db_);
  }

  void CloseDB() {
    if (db_) {
      db_->Flush(FlushOptions());
      delete db_;
      db_ = nullptr;
    }
  }

  CloudFileSystem* GetCloudFileSystem() const {
    EXPECT_TRUE(cloud_env_);
    return static_cast<CloudFileSystem*>(cloud_env_->GetFileSystem().get());
  }

  CloudFileSystemImpl* GetCloudFileSystemImpl() const {
    EXPECT_TRUE(cloud_env_);
    return static_cast<CloudFileSystemImpl*>(
        cloud_env_->GetFileSystem().get());
  }

  DBImpl* GetDBImpl() const { return static_cast<DBImpl*>(db_->GetBaseDB()); }

 protected:
  std::string test_id_;
  Env* base_env_;
  Options options_;
  std::string dbname_;
  std::string clone_dir_;
  CloudFileSystemOptions cloud_fs_options_;
  std::string dbid_;
  std::string persistent_cache_path_;
  uint64_t persistent_cache_size_gb_;
  DBCloud* db_;
  std::unique_ptr<Env> cloud_env_;
};

TEST_F(GcpCloudTest, BasicTest) {
  OpenDB();
  std::string value;
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
  value.clear();

  OpenDB();
  ASSERT_OK(db_->Get(ReadOptions(), "Hello", &value));
  ASSERT_EQ(value, "World");
  CloseDB();
}

TEST_F(GcpCloudTest, FindAllLiveFilesTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "Hello", "World"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  std::vector<std::string> tablefiles;
  std::string manifest;
  ASSERT_OK(GetCloudFileSystem()->FindAllLiveFiles(dbname_, &tablefiles,
                                                   nullptr, &manifest));
  EXPECT_GE(tablefiles.size(), 1);

  for (auto name : tablefiles) {
    EXPECT_EQ(GetFileType(name), RocksDBFileType::kSstFile);
    EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
        GetCloudFileSystem()->GetSrcBucketName(),
        GetCloudFileSystem()->GetSrcObjectPath() + pathsep + name));
  }

  EXPECT_EQ(GetFileType(manifest), RocksDBFileType::kManifestFile);
  EXPECT_OK(GetCloudFileSystem()->GetStorageProvider()->ExistsCloudObject(
      GetCloudFileSystem()->GetSrcBucketName(),
      GetCloudFileSystem()->GetSrcObjectPath() + pathsep + manifest));
}

TEST_F(GcpCloudTest, BlobDBCloudBasicTest) {
  options_.enable_blob_files = true;
  options_.min_blob_size = 0;
  OpenDB();

  ASSERT_OK(db_->Put(WriteOptions(), "BlobKey1", "BlobValue1"));
  ASSERT_OK(db_->Put(WriteOptions(), "BlobKey2", "BlobValue2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "BlobKey1", &value));
  ASSERT_EQ(value, "BlobValue1");
  ASSERT_OK(db_->Get(ReadOptions(), "BlobKey2", &value));
  ASSERT_EQ(value, "BlobValue2");
  CloseDB();
}

TEST_F(GcpCloudTest, MultipleKeysTest) {
  OpenDB();
  for (int i = 0; i < 100; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(i),
                        "val" + std::to_string(i)));
  }
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  for (int i = 0; i < 100; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "val" + std::to_string(i));
  }
  CloseDB();
}

TEST_F(GcpCloudTest, OverwriteTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ(value, "value2");
  CloseDB();
}

TEST_F(GcpCloudTest, DeleteTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "value1"));
  ASSERT_OK(db_->Flush(FlushOptions()));

  ASSERT_OK(db_->Delete(WriteOptions(), "key1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  std::string value;
  Status s = db_->Get(ReadOptions(), "key1", &value);
  ASSERT_TRUE(s.IsNotFound());
  CloseDB();
}

TEST_F(GcpCloudTest, CompactionTest) {
  OpenDB();
  for (int i = 0; i < 50; i++) {
    ASSERT_OK(db_->Put(WriteOptions(), "key" + std::to_string(i),
                        "val" + std::to_string(i)));
    if (i % 10 == 9) {
      ASSERT_OK(db_->Flush(FlushOptions()));
    }
  }
  GetDBImpl()->TEST_WaitForBackgroundWork();
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  for (int i = 0; i < 50; i++) {
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), "key" + std::to_string(i), &value));
    ASSERT_EQ(value, "val" + std::to_string(i));
  }
  CloseDB();
}

TEST_F(GcpCloudTest, ReopenTest) {
  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key1", "val1"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  ASSERT_OK(db_->Put(WriteOptions(), "key2", "val2"));
  ASSERT_OK(db_->Flush(FlushOptions()));
  GetDBImpl()->TEST_WaitForBackgroundWork();
  CloseDB();

  OpenDB();
  std::string value;
  ASSERT_OK(db_->Get(ReadOptions(), "key1", &value));
  ASSERT_EQ(value, "val1");
  ASSERT_OK(db_->Get(ReadOptions(), "key2", &value));
  ASSERT_EQ(value, "val2");
  CloseDB();
}

}  //  namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else  // USE_GCS

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr,
          "SKIPPED as GCP Cloud is supported only when USE_GCS is defined.\n");
  return 0;
}
#endif

#else  // ROCKSDB_LITE

#include <stdio.h>

int main(int, char**) {
  fprintf(stderr, "SKIPPED as DBCloud is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
