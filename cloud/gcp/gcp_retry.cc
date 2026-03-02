// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#include <chrono>

#include "rocksdb/cloud/cloud_file_system.h"

#ifdef USE_GCS
#include "cloud/gcp/gcp_file_system.h"

#include <google/cloud/options.h>
#include <google/cloud/retry_policy.h>
#include <google/cloud/storage/idempotency_policy.h>
#include <google/cloud/storage/options.h>
#include <google/cloud/storage/retry_policy.h>

namespace ROCKSDB_NAMESPACE {
namespace gcp = ::google::cloud;
namespace gcs = ::google::cloud::storage;

class GcpRetryPolicy : public gcs::RetryPolicy {
 public:
  template <typename DurationRep, typename DurationPeriod>
  explicit GcpRetryPolicy(
      CloudFileSystem* fs,
      std::chrono::duration<DurationRep, DurationPeriod> maximum_duration)
      : cfs_(fs),
        time_based_policy_(maximum_duration) {}

  std::chrono::milliseconds maximum_duration() const {
    return time_based_policy_.maximum_duration();
  }

  bool OnFailure(const gcp::Status& s) override {
    bool is_retryable = time_based_policy_.OnFailure(s);
    ++failure_count_;
    if (is_retryable) {
      if (failure_count_ <= maximum_failures_) {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
            "[gcs] Encountered failure: %s retried %d / %d times. Retrying...",
            s.message().c_str(), failure_count_, maximum_failures_);
        return true;
      } else {
        Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
            "[gcs] Encountered failure: %s retry attempt %d exceeds max "
            "retries %d. Aborting...",
            s.message().c_str(), failure_count_, maximum_failures_);
        return false;
      }
    } else {
      Log(InfoLogLevel::INFO_LEVEL, cfs_->GetLogger(),
          "[gcs] Encountered permanent failure: %s retry attempt %d / %d. "
          "Aborting...",
          s.message().c_str(), failure_count_, maximum_failures_);
      return false;
    }
  }

  bool IsExhausted() const override {
    return (time_based_policy_.IsExhausted() ||
            failure_count_ > maximum_failures_);
  }
  bool IsPermanentFailure(const gcp::Status& s) const override {
    return gcs::internal::StatusTraits::IsPermanentFailure(s);
  }

  std::unique_ptr<RetryPolicy> clone() const override {
    return std::make_unique<GcpRetryPolicy>(
        cfs_, time_based_policy_.maximum_duration());
  }

 private:
  int failure_count_ = 0;
  int maximum_failures_ = 10;
  CloudFileSystem* cfs_;
  gcp::internal::LimitedTimeRetryPolicy<gcs::internal::StatusTraits>
      time_based_policy_;
};

Status GcpCloudOptions::GetClientConfiguration(CloudFileSystem* fs,
                                               const std::string& /*region*/,
                                               gcp::Options& options) {
  uint64_t timeout_ms = 600000;
  options.set<gcs::IdempotencyPolicyOption>(
      gcs::AlwaysRetryIdempotencyPolicy().clone());

  options.set<gcs::BackoffPolicyOption>(
      gcs::ExponentialBackoffPolicy(std::chrono::milliseconds(500),
                                    std::chrono::minutes(1), 2.0)
          .clone());

  const auto& cloud_fs_options = fs->GetCloudFileSystemOptions();
  if (cloud_fs_options.request_timeout_ms != 0) {
    timeout_ms = cloud_fs_options.request_timeout_ms;
  }
  options.set<gcs::RetryPolicyOption>(
      GcpRetryPolicy(fs, std::chrono::milliseconds(timeout_ms)).clone());
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // USE_GCS
