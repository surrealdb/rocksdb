//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/local_sst_cache.h"

#include <thread>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "test_util/testharness.h"

namespace ROCKSDB_NAMESPACE {

class LocalSstCacheTest : public testing::Test {
 public:
  LocalSstCacheTest() {
    env_ = Env::Default();
    fs_ = env_->GetFileSystem();
    test_dir_ = test::TmpDir() + "/local_sst_cache_test";
    env_->CreateDirIfMissing(test_dir_);
  }

  ~LocalSstCacheTest() override {
    std::vector<std::string> children;
    env_->GetChildren(test_dir_, &children);
    for (const auto& child : children) {
      if (child != "." && child != "..") {
        env_->DeleteFile(test_dir_ + "/" + child);
      }
    }
    env_->DeleteDir(test_dir_);
  }

  void CreateLocalFile(const std::string& name, uint64_t size) {
    std::string path = test_dir_ + "/" + name;
    std::unique_ptr<FSWritableFile> f;
    ASSERT_OK(
        fs_->NewWritableFile(path, FileOptions(), &f, nullptr));
    std::string data(size, 'x');
    ASSERT_OK(f->Append(data, IOOptions(), nullptr));
    ASSERT_OK(f->Close(IOOptions(), nullptr));
  }

  bool FileExists(const std::string& name) {
    std::string path = test_dir_ + "/" + name;
    return fs_->FileExists(path, IOOptions(), nullptr).ok();
  }

 protected:
  Env* env_;
  std::shared_ptr<FileSystem> fs_;
  std::string test_dir_;
};

TEST_F(LocalSstCacheTest, BasicAddEvict) {
  // Cache limit of 200 bytes
  LocalSstCache cache(200, fs_, nullptr);

  std::string f1 = test_dir_ + "/000001.sst";
  std::string f2 = test_dir_ + "/000002.sst";
  std::string f3 = test_dir_ + "/000003.sst";

  CreateLocalFile("000001.sst", 100);
  CreateLocalFile("000002.sst", 100);
  CreateLocalFile("000003.sst", 100);

  cache.Add(f1, 100);
  ASSERT_EQ(cache.TotalSize(), 100);
  ASSERT_EQ(cache.NumEntries(), 1);

  cache.Add(f2, 100);
  ASSERT_EQ(cache.TotalSize(), 200);
  ASSERT_EQ(cache.NumEntries(), 2);

  // Adding f3 exceeds the limit -- f1 (oldest/coldest) should be evicted
  cache.Add(f3, 100);
  ASSERT_EQ(cache.TotalSize(), 200);
  ASSERT_EQ(cache.NumEntries(), 2);

  // f1 should have been deleted from local disk
  ASSERT_FALSE(FileExists("000001.sst"));
  ASSERT_TRUE(FileExists("000002.sst"));
  ASSERT_TRUE(FileExists("000003.sst"));
}

TEST_F(LocalSstCacheTest, TouchPromotes) {
  LocalSstCache cache(200, fs_, nullptr);

  std::string f1 = test_dir_ + "/000001.sst";
  std::string f2 = test_dir_ + "/000002.sst";
  std::string f3 = test_dir_ + "/000003.sst";

  CreateLocalFile("000001.sst", 100);
  CreateLocalFile("000002.sst", 100);
  CreateLocalFile("000003.sst", 100);

  cache.Add(f1, 100);
  cache.Add(f2, 100);

  // Touch f1, making f2 the coldest
  cache.Touch(f1);

  // Adding f3 should evict f2 (coldest), not f1
  cache.Add(f3, 100);
  ASSERT_EQ(cache.NumEntries(), 2);

  ASSERT_TRUE(FileExists("000001.sst"));
  ASSERT_FALSE(FileExists("000002.sst"));
  ASSERT_TRUE(FileExists("000003.sst"));
}

TEST_F(LocalSstCacheTest, RemoveReducesSize) {
  LocalSstCache cache(250, fs_, nullptr);

  std::string f1 = test_dir_ + "/000001.sst";
  std::string f2 = test_dir_ + "/000002.sst";
  std::string f3 = test_dir_ + "/000003.sst";

  CreateLocalFile("000001.sst", 100);
  CreateLocalFile("000002.sst", 100);
  CreateLocalFile("000003.sst", 100);

  cache.Add(f1, 100);
  cache.Add(f2, 100);

  // Remove f1 from tracking (simulating compaction delete)
  cache.Remove(f1);
  ASSERT_EQ(cache.TotalSize(), 100);
  ASSERT_EQ(cache.NumEntries(), 1);

  // Now adding f3 should fit without evicting f2
  cache.Add(f3, 100);
  ASSERT_EQ(cache.TotalSize(), 200);
  ASSERT_EQ(cache.NumEntries(), 2);

  // f2 should still be on disk (not evicted)
  ASSERT_TRUE(FileExists("000002.sst"));
  ASSERT_TRUE(FileExists("000003.sst"));
}

TEST_F(LocalSstCacheTest, RemoveNonexistent) {
  LocalSstCache cache(200, fs_, nullptr);
  // Should be a no-op, not crash
  cache.Remove(test_dir_ + "/nonexistent.sst");
  ASSERT_EQ(cache.TotalSize(), 0);
  ASSERT_EQ(cache.NumEntries(), 0);
}

TEST_F(LocalSstCacheTest, TouchNonexistent) {
  LocalSstCache cache(200, fs_, nullptr);
  // Should be a no-op, not crash
  cache.Touch(test_dir_ + "/nonexistent.sst");
  ASSERT_EQ(cache.TotalSize(), 0);
}

TEST_F(LocalSstCacheTest, AddUpdatesExisting) {
  LocalSstCache cache(500, fs_, nullptr);

  std::string f1 = test_dir_ + "/000001.sst";
  CreateLocalFile("000001.sst", 100);

  cache.Add(f1, 100);
  ASSERT_EQ(cache.TotalSize(), 100);

  // Re-add with different size (e.g. file was re-downloaded)
  cache.Add(f1, 150);
  ASSERT_EQ(cache.TotalSize(), 150);
  ASSERT_EQ(cache.NumEntries(), 1);
}

TEST_F(LocalSstCacheTest, SeedFromDirectory) {
  CreateLocalFile("000001.sst", 100);
  CreateLocalFile("000002.sst", 200);
  CreateLocalFile("000003.blob", 150);
  // Non-SST files should be ignored
  CreateLocalFile("LOG", 50);
  CreateLocalFile("MANIFEST-000001", 80);

  LocalSstCache cache(1000, fs_, nullptr);
  cache.SeedFromDirectory(test_dir_);

  // Should have found 3 files (2 SST + 1 blob)
  ASSERT_EQ(cache.NumEntries(), 3);
  ASSERT_EQ(cache.TotalSize(), 450);
}

TEST_F(LocalSstCacheTest, SeedAndEvict) {
  CreateLocalFile("000001.sst", 100);
  CreateLocalFile("000002.sst", 200);
  CreateLocalFile("000003.sst", 150);

  // Cache limit smaller than total -- should evict oldest files
  LocalSstCache cache(300, fs_, nullptr);
  cache.SeedFromDirectory(test_dir_);

  ASSERT_LE(cache.TotalSize(), 300);
  // At least one file should have been evicted
  ASSERT_LT(cache.NumEntries(), 3);
}

TEST_F(LocalSstCacheTest, MultipleEvictions) {
  LocalSstCache cache(100, fs_, nullptr);

  for (int i = 1; i <= 5; i++) {
    std::string name = "00000" + std::to_string(i) + ".sst";
    CreateLocalFile(name, 50);
    cache.Add(test_dir_ + "/" + name, 50);
  }

  // With 100 byte limit and 50-byte files, only 2 should remain
  ASSERT_EQ(cache.NumEntries(), 2);
  ASSERT_EQ(cache.TotalSize(), 100);

  // Only the 2 most recent files should exist
  ASSERT_FALSE(FileExists("000001.sst"));
  ASSERT_FALSE(FileExists("000002.sst"));
  ASSERT_FALSE(FileExists("000003.sst"));
  ASSERT_TRUE(FileExists("000004.sst"));
  ASSERT_TRUE(FileExists("000005.sst"));
}

TEST_F(LocalSstCacheTest, ConcurrentAccess) {
  LocalSstCache cache(5000, fs_, nullptr);

  // Create some files
  for (int i = 0; i < 20; i++) {
    char name[32];
    snprintf(name, sizeof(name), "%06d.sst", i);
    CreateLocalFile(name, 100);
  }

  std::vector<std::thread> threads;
  for (int t = 0; t < 4; t++) {
    threads.emplace_back([&cache, this, t]() {
      for (int i = 0; i < 100; i++) {
        int file_idx = (t * 100 + i) % 20;
        char name[64];
        snprintf(name, sizeof(name), "%s/%06d.sst", test_dir_.c_str(),
                 file_idx);
        std::string fname(name);

        switch (i % 3) {
          case 0:
            cache.Add(fname, 100);
            break;
          case 1:
            cache.Touch(fname);
            break;
          case 2:
            cache.Remove(fname);
            break;
        }
      }
    });
  }

  for (auto& th : threads) {
    th.join();
  }

  // Just verify no crashes and internal consistency
  ASSERT_LE(cache.TotalSize(), 5000);
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

#else

int main(int /*argc*/, char** /*argv*/) { return 0; }

#endif  // ROCKSDB_LITE
