//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#include "cloud/local_sst_cache.h"

#include <algorithm>
#include <cinttypes>
#include <vector>

#include "cloud/filename.h"
#include "rocksdb/env.h"
#include "rocksdb/io_status.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

LocalSstCache::LocalSstCache(uint64_t max_size,
                             std::shared_ptr<FileSystem> base_fs,
                             std::shared_ptr<Logger> logger)
    : max_size_(max_size),
      base_fs_(std::move(base_fs)),
      logger_(std::move(logger)) {}

void LocalSstCache::Add(const std::string& fname, uint64_t size) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = entries_.find(fname);
  if (it != entries_.end()) {
    total_size_ -= it->second.size;
    lru_list_.erase(it->second.lru_iter);
    entries_.erase(it);
  }
  lru_list_.push_front(fname);
  entries_[fname] = {size, lru_list_.begin()};
  total_size_ += size;
  MaybeEvict();
}

void LocalSstCache::Touch(const std::string& fname) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = entries_.find(fname);
  if (it == entries_.end()) {
    return;
  }
  lru_list_.erase(it->second.lru_iter);
  lru_list_.push_front(fname);
  it->second.lru_iter = lru_list_.begin();
}

void LocalSstCache::Remove(const std::string& fname) {
  std::lock_guard<std::mutex> lk(mutex_);
  auto it = entries_.find(fname);
  if (it == entries_.end()) {
    return;
  }
  total_size_ -= it->second.size;
  lru_list_.erase(it->second.lru_iter);
  entries_.erase(it);
}

void LocalSstCache::SeedFromDirectory(const std::string& dbname) {
  std::vector<std::string> children;
  const IOOptions io_opts;
  auto st = base_fs_->GetChildren(dbname, io_opts, &children, nullptr);
  if (!st.ok()) {
    Log(InfoLogLevel::WARN_LEVEL, logger_.get(),
        "[LocalSstCache] SeedFromDirectory failed to list %s: %s",
        dbname.c_str(), st.ToString().c_str());
    return;
  }

  struct FileInfo {
    std::string path;
    uint64_t size;
    uint64_t mtime;
  };
  std::vector<FileInfo> sst_files;

  for (const auto& child : children) {
    auto noepoch = RemoveEpoch(child);
    if (!IsSstFile(noepoch) && !IsBlobFile(noepoch)) {
      continue;
    }
    std::string path = dbname + "/" + child;
    uint64_t fsize = 0;
    auto s = base_fs_->GetFileSize(path, io_opts, &fsize, nullptr);
    if (!s.ok() || fsize == 0) {
      continue;
    }
    uint64_t mtime = 0;
    base_fs_->GetFileModificationTime(path, io_opts, &mtime, nullptr);
    sst_files.push_back({std::move(path), fsize, mtime});
  }

  // Sort oldest first so that the most recent files end up at LRU front.
  std::sort(sst_files.begin(), sst_files.end(),
            [](const FileInfo& a, const FileInfo& b) {
              return a.mtime < b.mtime;
            });

  std::lock_guard<std::mutex> lk(mutex_);
  for (const auto& fi : sst_files) {
    if (entries_.find(fi.path) != entries_.end()) {
      continue;
    }
    lru_list_.push_front(fi.path);
    entries_[fi.path] = {fi.size, lru_list_.begin()};
    total_size_ += fi.size;
  }
  MaybeEvict();

  Log(InfoLogLevel::INFO_LEVEL, logger_.get(),
      "[LocalSstCache] Seeded with %zu files, total %" PRIu64
      " bytes, limit %" PRIu64 " bytes",
      entries_.size(), total_size_, max_size_);
}

uint64_t LocalSstCache::TotalSize() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return total_size_;
}

size_t LocalSstCache::NumEntries() const {
  std::lock_guard<std::mutex> lk(mutex_);
  return entries_.size();
}

void LocalSstCache::MaybeEvict() {
  const IOOptions io_opts;
  while (total_size_ > max_size_ && !lru_list_.empty()) {
    const auto& victim = lru_list_.back();
    auto it = entries_.find(victim);
    if (it == entries_.end()) {
      lru_list_.pop_back();
      continue;
    }

    auto st = base_fs_->DeleteFile(victim, io_opts, nullptr);
    Log(InfoLogLevel::DEBUG_LEVEL, logger_.get(),
        "[LocalSstCache] Evicted %s (%" PRIu64 " bytes): %s", victim.c_str(),
        it->second.size, st.ToString().c_str());

    total_size_ -= it->second.size;
    entries_.erase(it);
    lru_list_.pop_back();
  }
}

}  // namespace ROCKSDB_NAMESPACE
