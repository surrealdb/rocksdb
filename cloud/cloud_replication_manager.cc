//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/cloud_replication_manager.h"

#include <cinttypes>

#include "cloud/cloud_scheduler.h"
#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE {

CloudReplicationManager::CloudReplicationManager(
    const CloudFileSystemOptions& cloud_opts,
    const std::shared_ptr<Logger>& logger)
    : cloud_opts_(cloud_opts), info_log_(logger) {}

CloudReplicationManager::~CloudReplicationManager() { Stop(); }

Status CloudReplicationManager::Initialize(CloudFileSystem* cfs) {
  scheduler_ = CloudScheduler::Get();

  // For each replication bucket, we reuse the primary storage provider.
  // The PutCloudObject interface accepts an explicit bucket_name parameter,
  // so the same S3/GCS client can write to any bucket. For cross-region
  // buckets, S3 handles request routing via 301 redirects automatically
  // when using path-style or virtual-hosted-style requests.
  auto primary_provider = cfs->GetStorageProvider();
  if (!primary_provider) {
    return Status::InvalidArgument(
        "Replication requires a storage provider");
  }

  for (const auto& bucket : cloud_opts_.replication_buckets) {
    ReplicationTarget target;
    target.bucket = bucket;
    target.provider = primary_provider;
    targets_.push_back(std::move(target));
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[replication] Initialized with %zu replication targets",
      targets_.size());
  return Status::OK();
}

void CloudReplicationManager::ScheduleReplication(
    const std::string& local_path, const std::string& cloud_name) {
  if (stopped_.load(std::memory_order_relaxed) || targets_.empty()) return;

  {
    std::lock_guard<std::mutex> lk(mu_);
    pending_[local_path] += static_cast<int>(targets_.size());
    file_refcounts_[local_path] += static_cast<int>(targets_.size());
  }

  for (size_t i = 0; i < targets_.size(); ++i) {
    auto* self = this;
    std::string lp = local_path;
    std::string cn = cloud_name;
    size_t idx = i;
    scheduler_->ScheduleJob(
        std::chrono::microseconds(0),
        [self, lp, cn, idx](void*) { self->DoReplicate(lp, cn, idx); },
        nullptr);
  }

  Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
      "[replication] Scheduled replication of %s to %zu targets",
      cloud_name.c_str(), targets_.size());
}

void CloudReplicationManager::DoReplicate(const std::string& local_path,
                                          const std::string& cloud_name,
                                          size_t target_idx) {
  if (stopped_.load(std::memory_order_relaxed)) return;
  if (target_idx >= targets_.size()) return;

  const auto& target = targets_[target_idx];
  auto object_path =
      target.bucket.GetObjectPath() + "/" + basename(cloud_name);

  auto st = target.provider->PutCloudObject(
      local_path, target.bucket.GetBucketName(), object_path);

  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[replication] Failed to replicate %s to bucket %s: %s",
        cloud_name.c_str(), target.bucket.GetBucketName().c_str(),
        st.ToString().c_str());
  } else {
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "[replication] Replicated %s to bucket %s",
        cloud_name.c_str(), target.bucket.GetBucketName().c_str());
  }

  bool should_delete = false;
  {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = pending_.find(local_path);
    if (it != pending_.end()) {
      it->second--;
      if (it->second <= 0) {
        pending_.erase(it);
      }
    }

    auto rc = file_refcounts_.find(local_path);
    if (rc != file_refcounts_.end()) {
      rc->second--;
      if (rc->second <= 0) {
        file_refcounts_.erase(rc);
        should_delete = !cloud_opts_.keep_local_sst_files;
      }
    }
    cv_.notify_all();
  }

  if (should_delete) {
    auto del_st = Env::Default()->GetFileSystem()->DeleteFile(
        local_path, IOOptions(), nullptr);
    if (del_st.ok()) {
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[replication] Deferred delete of %s after replication complete",
          local_path.c_str());
    }
  }
}

IOStatus CloudReplicationManager::WaitForAllPending() {
  std::unique_lock<std::mutex> lk(mu_);
  cv_.wait(lk, [this]() { return pending_.empty() || stopped_.load(); });
  if (stopped_.load()) {
    return IOStatus::Aborted("Replication manager stopped");
  }
  return IOStatus::OK();
}

IOStatus CloudReplicationManager::ReplicateManifestAndCloudManifest(
    const std::string& local_dbname, const std::string& epoch,
    const std::string& cookie, const std::string& dest_object_path) {
  for (const auto& target : targets_) {
    auto manifest_local = ManifestFileWithEpoch(local_dbname, epoch);
    auto manifest_cloud =
        ManifestFileWithEpoch(target.bucket.GetObjectPath(), epoch);

    auto st = target.provider->PutCloudObject(
        manifest_local, target.bucket.GetBucketName(), manifest_cloud);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[replication] Failed to replicate MANIFEST-%s to bucket %s: %s",
          epoch.c_str(), target.bucket.GetBucketName().c_str(),
          st.ToString().c_str());
      return st;
    }

    auto cm_local = MakeCloudManifestFile(local_dbname, cookie);
    auto cm_cloud = MakeCloudManifestFile(target.bucket.GetObjectPath(), cookie);

    st = target.provider->PutCloudObject(
        cm_local, target.bucket.GetBucketName(), cm_cloud);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[replication] Failed to replicate CLOUDMANIFEST-%s to bucket %s: %s",
          cookie.c_str(), target.bucket.GetBucketName().c_str(),
          st.ToString().c_str());
      return st;
    }

    Log(InfoLogLevel::INFO_LEVEL, info_log_,
        "[replication] Replicated MANIFEST-%s and CLOUDMANIFEST to bucket %s",
        epoch.c_str(), target.bucket.GetBucketName().c_str());
  }
  return IOStatus::OK();
}

void CloudReplicationManager::ScheduleDeletion(const std::string& cloud_name) {
  if (stopped_.load(std::memory_order_relaxed) || targets_.empty()) return;

  for (const auto& target : targets_) {
    auto object_path =
        target.bucket.GetObjectPath() + "/" + basename(cloud_name);
    auto st = target.provider->DeleteCloudObject(
        target.bucket.GetBucketName(), object_path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[replication] Failed to delete %s from bucket %s: %s",
          cloud_name.c_str(), target.bucket.GetBucketName().c_str(),
          st.ToString().c_str());
    }
  }
}

bool CloudReplicationManager::HasPendingReplication(
    const std::string& local_path) const {
  std::lock_guard<std::mutex> lk(mu_);
  return file_refcounts_.count(local_path) > 0;
}

void CloudReplicationManager::Stop() {
  if (stopped_.exchange(true)) return;
  {
    std::lock_guard<std::mutex> lk(mu_);
    cv_.notify_all();
  }
  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[replication] CloudReplicationManager stopped");
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
