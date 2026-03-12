//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "rocksdb/file_system.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Logger;

// A bounded LRU cache for local SST/blob files in RocksDB-Cloud.
//
// When configured, SST files are kept locally after upload to cloud storage.
// The cache tracks total size of locally cached files and evicts the
// least-recently-accessed files when the total exceeds the configured limit.
// Evicted files remain in cloud storage and are re-downloaded on demand
// using the existing fallback path in CloudFileSystemImpl.
//
// Thread-safe: all public methods are protected by a mutex.
class LocalSstCache {
 public:
  LocalSstCache(uint64_t max_size, std::shared_ptr<FileSystem> base_fs,
                std::shared_ptr<Logger> logger);

  // Register a file in the cache. Inserts at LRU front.
  // If the file is already tracked, updates its size and moves to front.
  // Triggers eviction if total size exceeds the limit.
  void Add(const std::string& fname, uint64_t size);

  // Move a file to the LRU front (most recently used).
  // No-op if the file is not tracked.
  void Touch(const std::string& fname);

  // Remove a file from tracking (e.g., when compaction deletes it).
  // Does NOT delete the local file -- the caller handles that.
  void Remove(const std::string& fname);

  // Scan a directory for existing SST/blob files and seed the cache.
  // Files are added in mtime order (oldest first, so most recent is at
  // LRU front). Called during DB open to restore cache state.
  void SeedFromDirectory(const std::string& dbname);

  uint64_t TotalSize() const;
  uint64_t MaxSize() const { return max_size_; }
  size_t NumEntries() const;

 private:
  struct CacheEntry {
    uint64_t size;
    std::list<std::string>::iterator lru_iter;
  };

  // Evict least-recently-used files until total_size_ <= max_size_.
  // Caller must hold mutex_.
  void MaybeEvict();

  const uint64_t max_size_;
  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<Logger> logger_;

  mutable std::mutex mutex_;
  uint64_t total_size_ = 0;

  // Front = most recently used, back = least recently used.
  std::list<std::string> lru_list_;
  std::unordered_map<std::string, CacheEntry> entries_;
};

}  // namespace ROCKSDB_NAMESPACE
