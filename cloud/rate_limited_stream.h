//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//
#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <streambuf>

#include "rocksdb/env.h"
#include "rocksdb/rate_limiter.h"

namespace ROCKSDB_NAMESPACE {

// A std::streambuf wrapper that rate-limits read operations (xsgetn/underflow).
// Used to throttle S3/GCS upload streams: the SDK reads from this buffer,
// and each read blocks until the rate limiter grants tokens.
class RateLimitedReadStreamBuf : public std::streambuf {
 public:
  RateLimitedReadStreamBuf(std::streambuf* underlying, RateLimiter* limiter,
                           size_t chunk_size = 64 * 1024)
      : underlying_(underlying), limiter_(limiter), chunk_size_(chunk_size) {}

 protected:
  std::streamsize xsgetn(char* s, std::streamsize count) override {
    std::streamsize total = 0;
    while (total < count) {
      auto to_read =
          static_cast<std::streamsize>(std::min(static_cast<size_t>(count - total), chunk_size_));
      if (limiter_) {
        limiter_->Request(static_cast<int64_t>(to_read), Env::IOPriority::IO_LOW,
                          nullptr /* stats */, RateLimiter::OpType::kRead);
      }
      auto got = underlying_->sgetn(s + total, to_read);
      if (got <= 0) break;
      total += got;
    }
    return total;
  }

  int_type underflow() override { return underlying_->sgetc(); }

 private:
  std::streambuf* underlying_;
  RateLimiter* limiter_;
  size_t chunk_size_;
};

// A std::streambuf wrapper that rate-limits write operations (xsputn).
// Used to throttle S3/GCS download streams: as the SDK writes response
// data to this buffer, each write blocks until the rate limiter grants tokens.
class RateLimitedWriteStreamBuf : public std::streambuf {
 public:
  RateLimitedWriteStreamBuf(std::streambuf* underlying, RateLimiter* limiter,
                            size_t chunk_size = 64 * 1024)
      : underlying_(underlying), limiter_(limiter), chunk_size_(chunk_size) {}

 protected:
  std::streamsize xsputn(const char* s, std::streamsize count) override {
    std::streamsize total = 0;
    while (total < count) {
      auto to_write =
          static_cast<std::streamsize>(std::min(static_cast<size_t>(count - total), chunk_size_));
      if (limiter_) {
        limiter_->Request(static_cast<int64_t>(to_write), Env::IOPriority::IO_LOW,
                          nullptr /* stats */, RateLimiter::OpType::kWrite);
      }
      auto written = underlying_->sputn(s + total, to_write);
      if (written <= 0) break;
      total += written;
    }
    return total;
  }

  int_type overflow(int_type ch) override {
    return underlying_->sputc(static_cast<char>(ch));
  }

  int sync() override { return underlying_->pubsync(); }

 private:
  std::streambuf* underlying_;
  RateLimiter* limiter_;
  size_t chunk_size_;
};

// RAII wrapper that creates a rate-limited istream from an existing istream.
class RateLimitedInputStream : public std::istream {
 public:
  RateLimitedInputStream(std::istream* underlying, RateLimiter* limiter)
      : std::istream(nullptr),
        buf_(underlying->rdbuf(), limiter) {
    rdbuf(&buf_);
  }

 private:
  RateLimitedReadStreamBuf buf_;
};

// RAII wrapper that creates a rate-limited ostream from an existing ostream.
class RateLimitedOutputStream : public std::ostream {
 public:
  RateLimitedOutputStream(std::ostream* underlying, RateLimiter* limiter)
      : std::ostream(nullptr),
        buf_(underlying->rdbuf(), limiter) {
    rdbuf(&buf_);
  }

 private:
  RateLimitedWriteStreamBuf buf_;
};

}  // namespace ROCKSDB_NAMESPACE
