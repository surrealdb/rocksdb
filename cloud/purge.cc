// Copyright (c) 2017 Rockset.
// Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
#ifndef ROCKSDB_LITE

#include <chrono>
#include <set>

#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "file/filename.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"

namespace ROCKSDB_NAMESPACE {

// A map from a dbid to the list of all its parent dbids.
typedef std::map<std::string, std::vector<std::string>> DbidParents;

std::vector<std::string> CloudFileSystemImpl::BuildAncestorDbids(
    const std::string& dbid) {
  const std::string delimiter(DBID_SEPARATOR);
  std::vector<std::string> segments;
  size_t start = 0, end = 0;
  while (end != std::string::npos) {
    end = dbid.find(delimiter, start);
    segments.push_back(dbid.substr(
        start, (end == std::string::npos) ? std::string::npos : end - start));
    start = ((end > (std::string::npos - delimiter.size()))
                 ? std::string::npos
                 : end + delimiter.size());
  }

  std::vector<std::string> ancestors;
  std::string accumulated;
  for (size_t i = 0; i < segments.size(); ++i) {
    if (i == 0) {
      accumulated = segments[i];
    } else {
      accumulated += delimiter + segments[i];
    }
    ancestors.push_back(accumulated);
  }
  return ancestors;
}

//
// Keep running till running is true
//
void CloudFileSystemImpl::Purger() {
  // Run purge once every period.
  auto period = std::chrono::milliseconds(
      GetCloudFileSystemOptions().purger_periodicity_millis);

  std::vector<std::string> to_be_deleted_paths;
  std::vector<std::string> to_be_deleted_dbids;

  while (true) {
    std::unique_lock<std::mutex> lk(purger_lock_);
    purger_cv_.wait_for(lk, period, [&]() { return !purger_is_running_; });
    if (!purger_is_running_) {
      break;
    }
    // Delete the objects that were detected to be obsolete in the previous
    // run. This two-phase approach ensures that obsolete files are not
    // immediately deleted, giving clone-to-local-dir code time to copy
    // them at clone-creation time.

    for (const auto& p : to_be_deleted_dbids) {
      auto st = DeleteDbid(GetDestBucketName(), p);
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] bucket %s obsolete dbid %s deletion: %s",
          GetDestBucketName().c_str(), p.c_str(), st.ToString().c_str());
    }

    for (const auto& p : to_be_deleted_paths) {
      auto st = GetStorageProvider()->DeleteCloudObject(GetDestBucketName(), p);
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] bucket %s obsolete path %s deletion: %s",
          GetDestBucketName().c_str(), p.c_str(), st.ToString().c_str());
    }

    to_be_deleted_paths.clear();
    to_be_deleted_dbids.clear();
    FindObsoleteFiles(GetDestBucketName(), &to_be_deleted_paths);
    FindObsoleteDbid(GetDestBucketName(), &to_be_deleted_dbids);
  }
}

IOStatus CloudFileSystemImpl::FindObsoleteFiles(
    const std::string& bucket_name_prefix,
    std::vector<std::string>* pathnames) {
  std::set<std::string> live_files;

  // fetch list of all registered dbids
  DbidList dbid_list;
  auto st = GetDbidList(bucket_name_prefix, &dbid_list);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[pg] GetDbidList on bucket prefix %s. %s", bucket_name_prefix.c_str(),
        st.ToString().c_str());
    return st;
  }

  // For each of the dbids names, extract its list of parent-dbs
  DbidParents parents;
  st = extractParents(bucket_name_prefix, dbid_list, &parents);
  if (!st.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_,
        "[pg] extractParents on bucket prefix %s. %s",
        bucket_name_prefix.c_str(), st.ToString().c_str());
    return st;
  }

  std::unique_ptr<ManifestReader> extractor(
      new ManifestReader(info_log_, this, bucket_name_prefix));

  // Step2: from all MANIFEST files in Step 1, compile a list of all live files
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::set<uint64_t> file_nums;
    st = extractor->GetLiveFiles(iter->second, &file_nums);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] dbid %s extracted files from path %s %s", iter->first.c_str(),
          iter->second.c_str(), st.ToString().c_str());
    } else {
      // This file can reside either in this leaf db's path or reside in any of
      // the parent db's paths. Compute all possible paths and insert them into
      // live_files
      for (auto it = file_nums.begin(); it != file_nums.end(); ++it) {
        // list of all parent dbids
        const std::vector<std::string>& parent_dbids = parents[iter->first];

        for (const auto& db : parent_dbids) {
          // parent db's paths
          const std::string& parent_path = dbid_list[db];
          live_files.insert(MakeTableFileName(parent_path, *it));
        }
      }
    }
  }

  // Get all files from all dbpaths in this bucket
  std::vector<std::string> all_files;

  // Scan all the db directories in this bucket. Retrieve the list
  // of files in all these db directories.
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::unique_ptr<SequentialFile> result;
    std::string mpath = iter->second;

    std::vector<std::string> objects;
    st = GetStorageProvider()->ListCloudObjects(bucket_name_prefix, mpath,
                                                &objects);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to list objects in bucketprefix %s path_prefix %s. %s",
          bucket_name_prefix.c_str(), mpath.c_str(), st.ToString().c_str());
    }
    for (auto& o : objects) {
      all_files.push_back(mpath + "/" + o);
    }
  }

  // If a file does not belong to live_files, then it can be deleted
  for (const auto& candidate : all_files) {
    if (live_files.find(candidate) == live_files.end() &&
        ends_with(candidate, ".sst")) {
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
          "[pg] bucket prefix %s path %s marked for deletion",
          bucket_name_prefix.c_str(), candidate.c_str());
      pathnames->push_back(candidate);
    }
  }
  return IOStatus::OK();
}

IOStatus CloudFileSystemImpl::FindObsoleteDbid(
    const std::string& bucket_name_prefix,
    std::vector<std::string>* to_delete_list) {
  DbidList dbid_list;
  auto st = GetDbidList(bucket_name_prefix, &dbid_list);

  if (st.ok()) {
    for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
      std::string path = CloudManifestFile(iter->second);
      st = GetStorageProvider()->ExistsCloudObject(bucket_name_prefix, path);
      if (st.IsNotFound()) {
        to_delete_list->push_back(iter->first);
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[pg] dbid %s non-existent dbpath %s scheduled for deletion",
            iter->first.c_str(), iter->second.c_str());
        st = IOStatus::OK();
      }
    }
  }
  return st;
}

// For each of the dbids in the list, extract the entire list of
// parent dbids. The IDENTITY file contains a chain like:
//   "rootDbId<sep>clone1Suffix<sep>clone2Suffix"
// We reconstruct cumulative registered dbids from these segments:
//   ["rootDbId", "rootDbId<sep>clone1Suffix",
//    "rootDbId<sep>clone1Suffix<sep>clone2Suffix"]
// These cumulative strings match the dbids stored in the registry.
IOStatus CloudFileSystemImpl::extractParents(
    const std::string& bucket_name_prefix, const DbidList& dbid_list,
    DbidParents* parents) {
  std::string unique_suffix = Env::Default()->GenerateUniqueId();
  unique_suffix = trim(unique_suffix);
  const std::string scratch(SCRATCH_LOCAL_DIR);
  IOStatus st;
  for (auto iter = dbid_list.begin(); iter != dbid_list.end(); ++iter) {
    std::string cloudfile = iter->second + "/IDENTITY";
    std::string localfile = scratch + "/.cloud_IDENTITY." + unique_suffix;
    st = GetStorageProvider()->GetCloudObject(bucket_name_prefix, cloudfile,
                                              localfile);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to download IDENTITY file from "
          "bucket %s path %s. %s. Aborting...",
          bucket_name_prefix.c_str(), cloudfile.c_str(), st.ToString().c_str());
      return st;
    } else if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] Unable to download IDENTITY file from "
          "bucket %s path %s. %s. Skipping...",
          bucket_name_prefix.c_str(), cloudfile.c_str(), st.ToString().c_str());
      continue;
    }

    std::string all_dbid;
    st = ReadFileToString(base_fs_.get(), localfile, &all_dbid);
    // Always attempt to clean up the temp file
    auto del_st = base_fs_->DeleteFile(localfile, IOOptions(), nullptr);
    if (!st.ok()) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "[pg] Unable to read %s %s",
          localfile.c_str(), st.ToString().c_str());
      return st;
    }
    if (!del_st.ok()) {
      Log(InfoLogLevel::WARN_LEVEL, info_log_,
          "[pg] Unable to delete temp file %s %s", localfile.c_str(),
          del_st.ToString().c_str());
    }

    all_dbid = rtrim_if(trim(all_dbid), '\n');

    if (all_dbid != iter->first) {
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
          "[pg] IDENTITY mismatch for dbid '%s': file contains '%s'",
          iter->first.c_str(), all_dbid.c_str());
      return IOStatus::Corruption(
          "IDENTITY file content does not match registered dbid");
    }

    (*parents)[iter->first] = BuildAncestorDbids(all_dbid);
  }
  return st;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
