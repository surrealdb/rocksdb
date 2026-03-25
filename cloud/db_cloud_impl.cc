// Copyright (c) 2017 Rockset.
#ifndef ROCKSDB_LITE

#include "cloud/db_cloud_impl.h"

#include <cinttypes>

#include "cloud/cloud_branch.h"
#include "cloud/cloud_manifest.h"
#include "cloud/cloud_wal_controller.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/xxhash.h"
#include "utilities/persistent_cache/block_cache_tier.h"

namespace ROCKSDB_NAMESPACE {

namespace {
/**
 * This ConstantSstFileManager uses the same size for every sst files added.
 */
class ConstantSizeSstFileManager : public SstFileManagerImpl {
 public:
  ConstantSizeSstFileManager(int64_t constant_file_size,
                             const std::shared_ptr<SystemClock>& clock,
                             const std::shared_ptr<FileSystem>& fs,
                             std::shared_ptr<Logger> logger,
                             int64_t rate_bytes_per_sec,
                             double max_trash_db_ratio,
                             uint64_t bytes_max_delete_chunk)
      : SstFileManagerImpl(clock, fs, std::move(logger), rate_bytes_per_sec,
                           max_trash_db_ratio, bytes_max_delete_chunk),
        constant_file_size_(constant_file_size) {
    assert(constant_file_size_ >= 0);
  }

  Status OnAddFile(const std::string& file_path) {
    return SstFileManagerImpl::OnAddFile(file_path,
                                         uint64_t(constant_file_size_));
  }

 private:
  const int64_t constant_file_size_;
};
}  // namespace

DBCloudImpl::DBCloudImpl(DB* db, std::unique_ptr<Env> local_env)
    : DBCloud(db), cfs_(nullptr), local_env_(std::move(local_env)) {}

DBCloudImpl::~DBCloudImpl() {}

Status DBCloud::Open(const Options& options, const std::string& dbname,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb, DBCloud** dbptr,
                     bool read_only) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  DBCloud* dbcloud = nullptr;
  Status s =
      DBCloud::Open(options, dbname, column_families, persistent_cache_path,
                    persistent_cache_size_gb, &handles, &dbcloud, read_only);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
    *dbptr = dbcloud;
  }
  return s;
}

Status DBCloud::Open(const Options& opt, const std::string& local_dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles, DBCloud** dbptr,
                     bool read_only) {
  Status st;
  Options options = opt;

  // Created logger if it is not already pre-created by user.
  if (!options.info_log) {
    CreateLoggerFromOptions(local_dbname, options, &options.info_log);
  }

  auto* cfs =
      static_cast<CloudFileSystem*>(options.env->GetFileSystem().get());
  assert(cfs);
  if (!cfs->GetLogger()) {
    cfs->SetLogger(options.info_log);
  }
  // Use a constant sized SST File Manager if necesary.
  // NOTE: if user already passes in an SST File Manager, we will respect user's
  // SST File Manager instead.
  auto constant_sst_file_size = cfs->GetCloudFileSystemOptions()
                                    .constant_sst_file_size_in_sst_file_manager;
  if (constant_sst_file_size >= 0 && options.sst_file_manager == nullptr) {
    // rate_bytes_per_sec, max_trash_db_ratio, bytes_max_delete_chunk are
    // default values in NewSstFileManager.
    // If users don't use Options.sst_file_manager, then these values are used
    // currently when creating an SST File Manager.
    options.sst_file_manager = std::make_shared<ConstantSizeSstFileManager>(
        constant_sst_file_size, options.env->GetSystemClock(),
        options.env->GetFileSystem(), options.info_log,
        0 /* rate_bytes_per_sec */, 0.25 /* max_trash_db_ratio */,
        64 * 1024 * 1024 /* bytes_max_delete_chunk */);
  }

  const auto& local_fs = cfs->GetBaseFileSystem();
  const IOOptions io_opts;
  IODebugContext* dbg = nullptr;
  if (!read_only) {
    local_fs->CreateDirIfMissing(local_dbname, io_opts,
                                 dbg);  // MJR: TODO: Move into sanitize
  }

  bool new_db = false;
  // If cloud manifest is already loaded, this means the directory has been
  // sanitized (possibly by the call to ListColumnFamilies())
  if (cfs->GetCloudManifest() == nullptr) {
    st = cfs->SanitizeLocalDirectory(options, local_dbname, read_only);

    if (st.ok()) {
      st = cfs->LoadCloudManifest(local_dbname, read_only);
    }
    if (st.IsNotFound()) {
      Log(InfoLogLevel::INFO_LEVEL, options.info_log,
          "CLOUDMANIFEST not found in the cloud, assuming this is a new "
          "database");
      new_db = true;
      st = Status::OK();
    } else if (!st.ok()) {
      return st;
    }
  }
  if (new_db) {
    if (read_only || !options.create_if_missing) {
      return Status::NotFound(
          "CLOUDMANIFEST not found and not creating new db");
    }
    st = cfs->CreateCloudManifest(
        local_dbname, cfs->GetCloudFileSystemOptions().new_cookie_on_open);
    if (!st.ok()) {
      return st;
    }
  }

  // Local environment, to be owned by DBCloudImpl, so that it outlives the
  // cache object created below.
  std::unique_ptr<Env> local_env(
      new CompositeEnvWrapper(options.env, local_fs));

  // If a persistent cache path is specified, then we set it in the options.
  if (!persistent_cache_path.empty() && persistent_cache_size_gb) {
    // Get existing options. If the persistent cache is already set, then do
    // not make any change. Otherwise, configure it.
    auto* tableopt =
        options.table_factory->GetOptions<BlockBasedTableOptions>();
    if (tableopt != nullptr && !tableopt->persistent_cache) {
      PersistentCacheConfig config(
          local_env.get(), persistent_cache_path,
          persistent_cache_size_gb * 1024L * 1024L * 1024L, options.info_log);
      auto pcache = std::make_shared<BlockCacheTier>(config);
      st = pcache->Open();
      if (st.ok()) {
        tableopt->persistent_cache = pcache;
        Log(InfoLogLevel::INFO_LEVEL, options.info_log,
            "Created persistent cache %s with size %" PRIu64 "GB",
            persistent_cache_path.c_str(), persistent_cache_size_gb);
      } else {
        Log(InfoLogLevel::INFO_LEVEL, options.info_log,
            "Unable to create persistent cache %s. %s",
            persistent_cache_path.c_str(), st.ToString().c_str());
        return st;
      }
    }
  }
  // We do not want a very large MANIFEST file because the MANIFEST file is
  // uploaded to S3 for every update, so always enable rolling of Manifest file
  options.max_manifest_file_size = DBCloudImpl::max_manifest_file_size;

  std::unique_ptr<DB> db;
  std::string dbid;
  if (read_only) {
    st = DB::OpenForReadOnly(options, local_dbname, column_families, handles,
                             &db);
  } else {
    st = DB::Open(options, local_dbname, column_families, handles, &db);
  }

  if (new_db && st.ok() && cfs->HasDestBucket() &&
      cfs->GetCloudFileSystemOptions().roll_cloud_manifest_on_open) {
    // This is a new database, upload the CLOUDMANIFEST after all MANIFEST file
    // was already uploaded. It is at this point we consider the database
    // committed in the cloud.
    st = cfs->UploadCloudManifest(
        local_dbname, cfs->GetCloudFileSystemOptions().new_cookie_on_open);
  }

  // now that the database is opened, all file sizes have been verified and we
  // no longer need to verify file sizes for each file that we open. Note that
  // this might have a data race with background compaction, but it's not a big
  // deal, since it's a boolean and it does not impact correctness in any way.
  if (cfs->GetCloudFileSystemOptions().validate_filesize) {
    *const_cast<bool*>(&cfs->GetCloudFileSystemOptions().validate_filesize) =
        false;
  }

  if (st.ok()) {
    DBCloudImpl* cloud = new DBCloudImpl(db.release(), std::move(local_env));
    *dbptr = cloud;
    cloud->GetDbIdentity(dbid);
  }
  Log(InfoLogLevel::INFO_LEVEL, options.info_log,
      "Opened cloud db with local dir %s dbid %s. %s", local_dbname.c_str(),
      dbid.c_str(), st.ToString().c_str());
  return st;
}

Status DBCloudImpl::Savepoint() {
  std::string dbid;
  Options default_options = GetOptions();
  Status st = GetDbIdentity(dbid);
  if (!st.ok()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint could not get dbid %s", st.ToString().c_str());
    return st;
  }
  auto* cfs =
      static_cast<CloudFileSystemImpl*>(GetEnv()->GetFileSystem().get());
  assert(cfs);

  // If there is no destination bucket, then nothing to do
  if (!cfs->HasDestBucket()) {
    Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
        "Savepoint on cloud dbid %s has no destination bucket, nothing to do.",
        dbid.c_str());
    return st;
  }

  Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
      "Savepoint on cloud dbid  %s", dbid.c_str());

  // find all sst files in the db
  std::vector<LiveFileMetaData> live_files;
  GetLiveFilesMetaData(&live_files);

  auto provider = cfs->GetStorageProvider();
  // If an sst file does not exist in the destination path, then remember it
  std::vector<std::string> to_copy;
  for (auto onefile : live_files) {
    auto remapped_fname = cfs->RemapFilename(onefile.name);
    std::string destpath = cfs->GetDestObjectPath() + "/" + remapped_fname;
    if (!provider->ExistsCloudObject(cfs->GetDestBucketName(), destpath).ok()) {
      to_copy.push_back(remapped_fname);
    }
  }

  // copy all files in parallel
  std::atomic<size_t> next_file_meta_idx(0);
  int max_threads = default_options.max_file_opening_threads;

  std::function<void()> load_handlers_func = [&]() {
    while (true) {
      size_t idx = next_file_meta_idx.fetch_add(1);
      if (idx >= to_copy.size()) {
        break;
      }
      auto& onefile = to_copy[idx];
      auto s = provider->CopyCloudObject(
          cfs->GetSrcBucketName(), cfs->GetSrcObjectPath() + "/" + onefile,
          cfs->GetDestBucketName(), cfs->GetDestObjectPath() + "/" + onefile);
      if (!s.ok()) {
        Log(InfoLogLevel::INFO_LEVEL, default_options.info_log,
            "Savepoint on cloud dbid  %s error in copying srcbucket %s srcpath "
            "%s dest bucket %s dest path %s. %s",
            dbid.c_str(), cfs->GetSrcBucketName().c_str(),
            cfs->GetSrcObjectPath().c_str(), cfs->GetDestBucketName().c_str(),
            cfs->GetDestObjectPath().c_str(), s.ToString().c_str());
        if (st.ok()) {
          st = s;  // save at least one error
        }
        break;
      }
    }
  };

  if (max_threads <= 1) {
    load_handlers_func();
  } else {
    std::vector<port::Thread> threads;
    for (int i = 0; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    for (auto& t : threads) {
      t.join();
    }
  }
  return st;
}

Status DBCloudImpl::CaptureForkPoint(ForkPoint* result) {
  auto* cfs =
      static_cast<CloudFileSystemImpl*>(GetEnv()->GetFileSystem().get());
  assert(cfs);

  DisableFileDeletions();

  uint64_t max_file_number = 0;
  auto st = cfs->GetMaxFileNumberFromCurrentManifest(GetName(),
                                                     &max_file_number);
  if (st.ok()) {
    result->epoch = cfs->GetCloudManifest()->GetCurrentEpoch();
    result->file_number = max_file_number;
    auto& opts = cfs->GetCloudFileSystemOptions();
    result->cloud_manifest_cookie = opts.new_cookie_on_open.empty()
                                        ? opts.cookie_on_open
                                        : opts.new_cookie_on_open;
  }

  EnableFileDeletions();

  Log(InfoLogLevel::INFO_LEVEL, GetOptions().info_log,
      "CaptureForkPoint epoch %s file_number %" PRIu64 " cookie %s: %s",
      result->epoch.c_str(), result->file_number,
      result->cloud_manifest_cookie.c_str(), st.ToString().c_str());
  return st;
}

Status DBCloudImpl::CreateBranch(const BucketOptions& destination,
                                 const CreateBranchOptions& options,
                                 BranchInfo* result) {
  auto* cfs =
      static_cast<CloudFileSystemImpl*>(GetEnv()->GetFileSystem().get());
  assert(cfs);
  auto provider = cfs->GetStorageProvider();
  auto& base_fs = cfs->GetBaseFileSystem();
  auto& opts = cfs->GetCloudFileSystemOptions();

  if (options.flush_memtable) {
    FlushOptions fo;
    fo.wait = true;
    auto st = Flush(fo);
    if (!st.ok()) return st;
  }

  DisableFileDeletions();

  ForkPoint fp;
  auto st = CaptureForkPoint(&fp);
  if (!st.ok()) {
    EnableFileDeletions();
    return st;
  }

  // Clone the CloudManifest and set the parent reference
  auto child_manifest = cfs->GetCloudManifest()->clone();
  child_manifest->SetParentObjectPath(cfs->GetDestObjectPath());

  // Generate child DBID
  std::string parent_dbid;
  st = GetDbIdentity(parent_dbid);
  if (!st.ok()) {
    EnableFileDeletions();
    return st;
  }
  std::string child_dbid =
      parent_dbid + std::string(CloudFileSystemImpl::DBID_SEPARATOR) +
      Env::Default()->GenerateUniqueId();
  child_dbid = trim(child_dbid);

  // Write child CLOUDMANIFEST to a temp file and upload
  auto cookie = opts.new_cookie_on_open.empty() ? opts.cookie_on_open
                                                 : opts.new_cookie_on_open;
  auto cm_local = std::string("/tmp/.cloud_branch_cm_") +
                  trim(Env::Default()->GenerateUniqueId());
  {
    std::unique_ptr<WritableFileWriter> writer;
    st = WritableFileWriter::Create(base_fs, cm_local, FileOptions(), &writer,
                                    nullptr);
    if (st.ok()) {
      st = child_manifest->WriteToLog(std::move(writer));
    }
  }
  if (st.ok()) {
    st = provider->PutCloudObject(
        cm_local, destination.GetBucketName(),
        MakeCloudManifestFile(destination.GetObjectPath(), cookie));
  }
  base_fs->DeleteFile(cm_local, IOOptions(), nullptr);
  if (!st.ok()) {
    EnableFileDeletions();
    return st;
  }

  // Write child IDENTITY
  auto id_local = std::string("/tmp/.cloud_branch_id_") +
                  trim(Env::Default()->GenerateUniqueId());
  {
    std::unique_ptr<FSWritableFile> f;
    st = base_fs->NewWritableFile(id_local, FileOptions(), &f, nullptr);
    if (st.ok()) {
      st = f->Append(Slice(child_dbid), IOOptions(), nullptr);
    }
    if (st.ok()) {
      st = f->Close(IOOptions(), nullptr);
    }
  }
  if (st.ok()) {
    st = provider->PutCloudObject(
        id_local, destination.GetBucketName(),
        destination.GetObjectPath() + "/IDENTITY");
  }
  base_fs->DeleteFile(id_local, IOOptions(), nullptr);
  if (!st.ok()) {
    EnableFileDeletions();
    return st;
  }

  // Register the child DBID
  if (st.ok()) {
    st = cfs->SaveDbid(destination.GetBucketName(), child_dbid,
                       destination.GetObjectPath());
  }

  // Upload MANIFEST for the child
  if (st.ok()) {
    auto epoch = cfs->GetCloudManifest()->GetCurrentEpoch();
    auto manifest_local = ManifestFileWithEpoch(GetName(), epoch);
    st = provider->PutCloudObject(
        manifest_local, destination.GetBucketName(),
        ManifestFileWithEpoch(destination.GetObjectPath(), epoch));
  }

  // Copy WAL files if requested
  if (st.ok() && !options.flush_memtable && options.include_wal &&
      opts.background_wal_sync_to_cloud) {
    // Trigger synchronous upload of current WAL files
    std::vector<std::string> children;
    auto ls = base_fs->GetChildren(GetName(), IOOptions(), &children, nullptr);
    if (ls.ok()) {
      for (const auto& child : children) {
        if (!IsWalFile(child)) continue;
        auto local_path = GetName() + "/" + child;
        auto cloud_path = cfs->GetDestObjectPath() + "/wal/" + child;
        provider->PutCloudObject(local_path, cfs->GetDestBucketName(),
                                 cloud_path);
      }
    }

    // Server-side copy WAL files from parent path to child path
    std::string wal_prefix = cfs->GetDestObjectPath() + "/wal/";
    std::vector<std::string> wal_objects;
    ls = provider->ListCloudObjects(cfs->GetDestBucketName(), wal_prefix,
                                    &wal_objects);
    if (ls.ok()) {
      for (const auto& wal_obj : wal_objects) {
        if (!IsWalFile(wal_obj)) continue;
        auto src_path = wal_prefix + wal_obj;
        auto dst_path = destination.GetObjectPath() + "/wal/" + wal_obj;
        provider->CopyCloudObject(cfs->GetDestBucketName(), src_path,
                                  destination.GetBucketName(), dst_path);
      }
    }
  }

  // Write ref object in parent's path
  auto now = static_cast<uint64_t>(
      Env::Default()->GetCurrentTime(nullptr).ok()
          ? 0
          : 0);
  {
    auto env = Env::Default();
    uint64_t t;
    env->GetCurrentTime(reinterpret_cast<int64_t*>(&t));
    now = t;
  }

  BranchInfo info;
  info.dbid = child_dbid;
  info.object_path = destination.GetObjectPath();
  info.bucket_name = destination.GetBucketName();
  info.fork_file_number = fp.file_number;
  info.fork_epoch = fp.epoch;
  info.created_at = now;

  if (st.ok()) {
    st = CloudBranchUtil::WriteRefObject(provider, cfs->GetDestBucketName(),
                                         cfs->GetDestObjectPath(), info,
                                         base_fs);
  }

  // Update branch registry
  if (st.ok()) {
    std::vector<BranchInfo> branches;
    CloudBranchUtil::ReadBranchRegistry(provider, cfs->GetDestBucketName(),
                                        cfs->GetDestObjectPath(), &branches,
                                        base_fs);
    branches.push_back(info);
    CloudBranchUtil::WriteBranchRegistry(provider, cfs->GetDestBucketName(),
                                         cfs->GetDestObjectPath(), branches,
                                         base_fs);
  }

  EnableFileDeletions();

  if (st.ok()) {
    *result = info;
  }

  Log(InfoLogLevel::INFO_LEVEL, GetOptions().info_log,
      "CreateBranch to %s/%s: dbid=%s fork_file=%" PRIu64 " epoch=%s: %s",
      destination.GetBucketName().c_str(), destination.GetObjectPath().c_str(),
      child_dbid.c_str(), fp.file_number, fp.epoch.c_str(),
      st.ToString().c_str());
  return st;
}

Status DBCloudImpl::DetachBranch() {
  auto* cfs =
      static_cast<CloudFileSystemImpl*>(GetEnv()->GetFileSystem().get());
  assert(cfs);
  auto provider = cfs->GetStorageProvider();
  auto& base_fs = cfs->GetBaseFileSystem();

  auto parent_path = cfs->GetCloudManifest()->GetParentObjectPath();
  if (parent_path.empty()) {
    return Status::InvalidArgument("This database is not a branch");
  }

  DisableFileDeletions();

  // Find all live SST files
  std::vector<LiveFileMetaData> live_files;
  GetLiveFilesMetaData(&live_files);

  // Copy SSTs that are in the parent's path to our own path
  Status st;
  for (const auto& file : live_files) {
    auto remapped_fname = cfs->RemapFilename(file.name);
    auto dest_path = cfs->GetDestObjectPath() + "/" + remapped_fname;

    // Check if the file exists in our dest bucket
    auto exists =
        provider->ExistsCloudObject(cfs->GetDestBucketName(), dest_path);
    if (exists.ok()) continue;

    // Try to copy from parent path
    auto src_path = parent_path + "/" + remapped_fname;
    st = provider->CopyCloudObject(cfs->GetDestBucketName(), src_path,
                                   cfs->GetDestBucketName(), dest_path);
    if (!st.ok() && !st.IsNotFound()) {
      Log(InfoLogLevel::ERROR_LEVEL, GetOptions().info_log,
          "DetachBranch: failed to copy %s -> %s: %s", src_path.c_str(),
          dest_path.c_str(), st.ToString().c_str());
      EnableFileDeletions();
      return st;
    }

    // Also check fallback buckets
    if (st.IsNotFound()) {
      for (const auto& fb :
           cfs->GetCloudFileSystemOptions().fallback_buckets) {
        src_path = fb.GetObjectPath() + "/" + remapped_fname;
        st = provider->CopyCloudObject(fb.GetBucketName(), src_path,
                                       cfs->GetDestBucketName(), dest_path);
        if (st.ok()) break;
      }
    }
  }

  // Delete the ref from the parent
  std::string dbid;
  st = GetDbIdentity(dbid);
  if (st.ok()) {
    // Try parent bucket (same as dest for typical setup)
    CloudBranchUtil::DeleteRefObject(provider, cfs->GetDestBucketName(),
                                     parent_path, dbid);
  }

  // Clear the parent ref in our CloudManifest
  cfs->GetCloudManifest()->SetParentObjectPath("");

  EnableFileDeletions();

  Log(InfoLogLevel::INFO_LEVEL, GetOptions().info_log,
      "DetachBranch from parent %s: %s", parent_path.c_str(),
      st.ToString().c_str());
  return st;
}

Status DBCloudImpl::ListBranches(std::vector<BranchInfo>* branches) {
  auto* cfs =
      static_cast<CloudFileSystemImpl*>(GetEnv()->GetFileSystem().get());
  assert(cfs);
  auto provider = cfs->GetStorageProvider();
  auto& base_fs = cfs->GetBaseFileSystem();

  return CloudBranchUtil::ReadBranchRegistry(provider, cfs->GetDestBucketName(),
                                             cfs->GetDestObjectPath(), branches,
                                             base_fs);
}

Status DBCloudImpl::CheckpointToCloud(const BucketOptions& destination,
                                      const CheckpointToCloudOptions& options) {
  DisableFileDeletions();
  auto st = DoCheckpointToCloud(destination, options);
  EnableFileDeletions();
  return st;
}

Status DBCloudImpl::DoCheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  std::vector<std::string> live_files;
  uint64_t manifest_file_size{0};
  auto* cfs = static_cast<CloudFileSystem*>(GetEnv()->GetFileSystem().get());
  assert(cfs);
  const auto& local_fs = cfs->GetBaseFileSystem();

  auto st =
      GetLiveFiles(live_files, &manifest_file_size, options.flush_memtable);
  if (!st.ok()) {
    return st;
  }

  // Create a temp MANIFEST file first as this captures all the files we need
  auto current_epoch = cfs->GetCloudManifest()->GetCurrentEpoch();
  auto manifest_fname = ManifestFileWithEpoch(current_epoch);
  auto tmp_manifest_fname = manifest_fname + ".tmp";
  st = CopyFile(local_fs.get(), GetName() + "/" + manifest_fname,
                Temperature::kUnknown, GetName() + "/" + tmp_manifest_fname,
                Temperature::kUnknown, manifest_file_size, false, nullptr);
  if (!st.ok()) {
    return st;
  }

  std::vector<std::pair<std::string, std::string>> files_to_copy;
  for (auto& f : live_files) {
    uint64_t number = 0;
    FileType type;
    auto ok = ParseFileName(f, &number, &type);
    if (!ok) {
      return Status::InvalidArgument("Unknown file " + f);
    }
    if (type != kTableFile) {
      // ignore
      continue;
    }
    auto remapped_fname = cfs->RemapFilename(f);
    files_to_copy.emplace_back(remapped_fname, remapped_fname);
  }

  // IDENTITY file
  std::string dbid;
  st = ReadFileToString(cfs, IdentityFileName(GetName()), &dbid);
  if (!st.ok()) {
    return st;
  }
  dbid = rtrim_if(trim(dbid), '\n');
  files_to_copy.emplace_back(IdentityFileName(""), IdentityFileName(""));

  std::atomic<size_t> next_file_to_copy{0};
  int thread_count = std::max(1, options.thread_count);
  std::vector<Status> thread_statuses;
  thread_statuses.resize(thread_count);

  auto upload_file = [&](const std::shared_ptr<CloudStorageProvider>& provider,
                         const std::string& localName,
                         const std::string& destName) {
    return provider->PutCloudObject(
        GetName() + "/" + localName, destination.GetBucketName(),
        destination.GetObjectPath() + "/" + destName);
  };
  auto do_copy = [&](size_t threadId) {
    auto provider = cfs->GetStorageProvider();
    while (true) {
      size_t idx = next_file_to_copy.fetch_add(1);
      if (idx >= files_to_copy.size()) {
        break;
      }

      auto& f = files_to_copy[idx];
      auto copy_st = upload_file(provider, f.first, f.second);
      if (!copy_st.ok()) {
        thread_statuses[threadId] = std::move(copy_st);
        break;
      }
    }
  };

  if (thread_count == 1) {
    do_copy(0);
  } else {
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; ++i) {
      threads.emplace_back([&, i]() { do_copy(i); });
    }
    for (auto& t : threads) {
      t.join();
    }
  }

  for (auto& s : thread_statuses) {
    if (!s.ok()) {
      st = s;
      break;
    }
  }

  if (!st.ok()) {
    return st;
  }

  // Upload MANIFEST and CLOUDMANIFEST sequentially only after copying all data
  // files

  // MANIFEST file
  st = upload_file(cfs->GetStorageProvider(), tmp_manifest_fname,
                   manifest_fname);
  if (!st.ok()) {
    return st;
  }

  // CLOUDMANIFEST file
  st = upload_file(cfs->GetStorageProvider(), cfs->CloudManifestFile(""),
                   cfs->CloudManifestFile(""));
  if (!st.ok()) {
    return st;
  }

  // Ignore errors
  local_fs->DeleteFile(tmp_manifest_fname, IOOptions(), nullptr /*dbg*/);

  st = cfs->SaveDbid(destination.GetBucketName(), dbid,
                     destination.GetObjectPath());
  return st;
}

Status DBCloud::ListColumnFamilies(const DBOptions& db_options,
                                   const std::string& name,
                                   std::vector<std::string>* column_families) {
  auto* cfs =
      static_cast<CloudFileSystem*>(db_options.env->GetFileSystem().get());
  assert(cfs);

  cfs->GetBaseFileSystem()->CreateDirIfMissing(name, IOOptions(),
                                               nullptr /*dbg*/);

  auto st = cfs->SanitizeLocalDirectory(db_options, name, false);
  if (st.ok()) {
    st = cfs->LoadCloudManifest(name, false);
  }
  if (st.ok()) {
    st = status_to_io_status(
        DB::ListColumnFamilies(db_options, name, column_families));
  }

  return st;
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
