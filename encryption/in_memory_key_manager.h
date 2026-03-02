// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
// Copyright (c) 2025-present, SurrealDB Ltd.  All rights reserved.

#pragma once
#ifdef OPENSSL
#include <openssl/aes.h>

#include <string>
#include <unordered_map>

#include "encryption/encryption.h"
#include "port/port.h"
#include "test_util/testutil.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {
namespace encryption {

// KeyManager that stores metadata in memory. For tests and db_bench only.
class InMemoryKeyManager final : public KeyManager {
 public:
  explicit InMemoryKeyManager(EncryptionMethod method)
      : rnd_(42),
        method_(method),
        key_(rnd_.HumanReadableString(static_cast<int>(KeySize(method)))) {
    assert(method != EncryptionMethod::kUnknown);
  }

  virtual ~InMemoryKeyManager() = default;

  Status GetFile(const std::string& fname,
                 FileEncryptionInfo* file_info) override {
    assert(file_info != nullptr);
    MutexLock l(&mu_);
    if (files_.count(fname) == 0) {
      return Status::Corruption("File not found: " + fname);
    }
    file_info->method = method_;
    file_info->key = key_;
    file_info->iv = files_[fname];
    return Status::OK();
  }

  Status NewFile(const std::string& fname,
                 FileEncryptionInfo* file_info) override {
    assert(file_info != nullptr);
    MutexLock l(&mu_);
    std::string iv = rnd_.HumanReadableString(AES_BLOCK_SIZE);
    files_[fname] = iv;
    file_info->method = method_;
    file_info->key = key_;
    file_info->iv = iv;
    return Status::OK();
  }

  Status DeleteFile(const std::string& fname) override {
    MutexLock l(&mu_);
    if (files_.count(fname) == 0) {
      return Status::Corruption("File not found: " + fname);
    }
    files_.erase(fname);
    return Status::OK();
  }

  Status LinkFile(const std::string& src_fname,
                  const std::string& dst_fname) override {
    MutexLock l(&mu_);
    if (files_.count(src_fname) == 0) {
      return Status::Corruption("File not found: " + src_fname);
    }
    files_[dst_fname] = files_[src_fname];
    return Status::OK();
  }

 private:
  mutable port::Mutex mu_;
  Random rnd_;
  const EncryptionMethod method_;
  const std::string key_;
  std::unordered_map<std::string, std::string> files_;
};

}  // namespace encryption
}  // namespace ROCKSDB_NAMESPACE

#endif  // OPENSSL
