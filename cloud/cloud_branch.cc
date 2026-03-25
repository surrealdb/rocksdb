//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifndef ROCKSDB_LITE

#include "cloud/cloud_branch.h"

#include <cinttypes>
#include <sstream>

#include "cloud/filename.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

namespace ROCKSDB_NAMESPACE {

namespace {
const char* kTmpDir = "/tmp";
}

std::string CloudBranchUtil::RefObjectPath(const std::string& parent_path,
                                           const std::string& child_dbid) {
  return parent_path + "/.refs/" + child_dbid;
}

std::string CloudBranchUtil::RegistryPath(const std::string& parent_path) {
  return parent_path + "/.branches";
}

std::string CloudBranchUtil::SerializeBranchInfo(const BranchInfo& info) {
  std::ostringstream oss;
  oss << "{\"child_dbid\":\"" << info.dbid << "\","
      << "\"child_object_path\":\"" << info.object_path << "\","
      << "\"bucket_name\":\"" << info.bucket_name << "\","
      << "\"fork_file_number\":" << info.fork_file_number << ","
      << "\"fork_epoch\":\"" << info.fork_epoch << "\","
      << "\"created_at\":" << info.created_at << "}";
  return oss.str();
}

// Minimal JSON parser for BranchInfo. Handles the known fixed schema.
static bool ExtractJsonString(const std::string& json, const std::string& key,
                              std::string* value) {
  auto needle = "\"" + key + "\":\"";
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  auto end = json.find('"', pos);
  if (end == std::string::npos) return false;
  *value = json.substr(pos, end - pos);
  return true;
}

static bool ExtractJsonUint64(const std::string& json, const std::string& key,
                              uint64_t* value) {
  auto needle = "\"" + key + "\":";
  auto pos = json.find(needle);
  if (pos == std::string::npos) return false;
  pos += needle.size();
  char* endptr = nullptr;
  *value = strtoull(json.c_str() + pos, &endptr, 10);
  return endptr != json.c_str() + pos;
}

IOStatus CloudBranchUtil::DeserializeBranchInfo(const std::string& json,
                                                BranchInfo* info) {
  bool ok = ExtractJsonString(json, "child_dbid", &info->dbid) &&
            ExtractJsonString(json, "child_object_path", &info->object_path) &&
            ExtractJsonString(json, "fork_epoch", &info->fork_epoch) &&
            ExtractJsonUint64(json, "fork_file_number",
                              &info->fork_file_number) &&
            ExtractJsonUint64(json, "created_at", &info->created_at);
  ExtractJsonString(json, "bucket_name", &info->bucket_name);
  if (!ok) {
    return IOStatus::Corruption("Failed to parse BranchInfo JSON");
  }
  return IOStatus::OK();
}

std::string CloudBranchUtil::SerializeBranchList(
    const std::vector<BranchInfo>& branches) {
  std::ostringstream oss;
  oss << "{\"branches\":[";
  for (size_t i = 0; i < branches.size(); ++i) {
    if (i > 0) oss << ",";
    oss << SerializeBranchInfo(branches[i]);
  }
  oss << "]}";
  return oss.str();
}

IOStatus CloudBranchUtil::DeserializeBranchList(
    const std::string& json, std::vector<BranchInfo>* branches) {
  branches->clear();
  // Find array start
  auto arr_start = json.find("[");
  if (arr_start == std::string::npos) {
    return IOStatus::Corruption("Invalid branch registry JSON");
  }

  size_t pos = arr_start + 1;
  while (pos < json.size()) {
    auto obj_start = json.find('{', pos);
    if (obj_start == std::string::npos) break;
    auto obj_end = json.find('}', obj_start);
    if (obj_end == std::string::npos) break;

    std::string obj_json = json.substr(obj_start, obj_end - obj_start + 1);
    BranchInfo info;
    auto st = DeserializeBranchInfo(obj_json, &info);
    if (!st.ok()) return st;
    branches->push_back(std::move(info));
    pos = obj_end + 1;
  }
  return IOStatus::OK();
}

IOStatus CloudBranchUtil::WriteStringToCloud(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& cloud_path,
    const std::string& content,
    const std::shared_ptr<FileSystem>& local_fs) {
  auto unique_id = Env::Default()->GenerateUniqueId();
  std::string tmp_path =
      std::string(kTmpDir) + "/.cloud_branch_" + trim(unique_id);

  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;

  // Write content to temp file
  {
    std::unique_ptr<FSWritableFile> f;
    auto st = local_fs->NewWritableFile(tmp_path, FileOptions(), &f, dbg);
    if (!st.ok()) return st;
    st = f->Append(Slice(content), io_opts, dbg);
    if (!st.ok()) return st;
    st = f->Close(io_opts, dbg);
    if (!st.ok()) return st;
  }

  // Upload to cloud
  auto st = provider->PutCloudObject(tmp_path, bucket, cloud_path);

  // Cleanup temp file
  local_fs->DeleteFile(tmp_path, io_opts, dbg);
  return st;
}

IOStatus CloudBranchUtil::ReadStringFromCloud(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& cloud_path,
    std::string* content,
    const std::shared_ptr<FileSystem>& local_fs) {
  auto unique_id = Env::Default()->GenerateUniqueId();
  std::string tmp_path =
      std::string(kTmpDir) + "/.cloud_branch_" + trim(unique_id);

  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;

  auto st = provider->GetCloudObject(bucket, cloud_path, tmp_path);
  if (!st.ok()) {
    return st;
  }

  st = ReadFileToString(local_fs.get(), tmp_path, content);
  local_fs->DeleteFile(tmp_path, io_opts, dbg);
  return st;
}

IOStatus CloudBranchUtil::WriteRefObject(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& parent_path,
    const BranchInfo& info,
    const std::shared_ptr<FileSystem>& local_fs) {
  std::string path = RefObjectPath(parent_path, info.dbid);
  std::string json = SerializeBranchInfo(info);
  return WriteStringToCloud(provider, bucket, path, json, local_fs);
}

IOStatus CloudBranchUtil::DeleteRefObject(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& parent_path,
    const std::string& child_dbid) {
  std::string path = RefObjectPath(parent_path, child_dbid);
  return provider->DeleteCloudObject(bucket, path);
}

IOStatus CloudBranchUtil::ListRefObjects(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& parent_path,
    std::vector<BranchInfo>* branches,
    const std::shared_ptr<FileSystem>& local_fs) {
  branches->clear();
  std::string refs_prefix = parent_path + "/.refs/";
  std::vector<std::string> objects;
  auto st = provider->ListCloudObjects(bucket, refs_prefix, &objects);
  if (!st.ok()) {
    if (st.IsNotFound()) return IOStatus::OK();
    return st;
  }

  IOStatus first_error;
  for (const auto& obj : objects) {
    std::string content;
    std::string full_path = refs_prefix + obj;
    st = ReadStringFromCloud(provider, bucket, full_path, &content, local_fs);
    if (!st.ok()) {
      if (st.IsNotFound()) {
        continue;
      }
      if (first_error.ok()) {
        first_error = st;
      }
      continue;
    }

    BranchInfo info;
    st = DeserializeBranchInfo(content, &info);
    if (st.ok()) {
      branches->push_back(std::move(info));
    }
  }
  return first_error.ok() ? IOStatus::OK() : first_error;
}

IOStatus CloudBranchUtil::WriteBranchRegistry(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& parent_path,
    const std::vector<BranchInfo>& branches,
    const std::shared_ptr<FileSystem>& local_fs) {
  std::string path = RegistryPath(parent_path);
  std::string json = SerializeBranchList(branches);
  return WriteStringToCloud(provider, bucket, path, json, local_fs);
}

IOStatus CloudBranchUtil::ReadBranchRegistry(
    const std::shared_ptr<CloudStorageProvider>& provider,
    const std::string& bucket, const std::string& parent_path,
    std::vector<BranchInfo>* branches,
    const std::shared_ptr<FileSystem>& local_fs) {
  std::string path = RegistryPath(parent_path);
  std::string content;
  auto st = ReadStringFromCloud(provider, bucket, path, &content, local_fs);
  if (!st.ok()) {
    if (st.IsNotFound()) {
      branches->clear();
      return IOStatus::OK();
    }
    return st;
  }
  return DeserializeBranchList(content, branches);
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_LITE
