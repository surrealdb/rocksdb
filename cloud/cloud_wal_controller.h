//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "rocksdb/cloud/cloud_file_system.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class CloudFileSystem;
class CloudScheduler;

// Serialization helpers for WAL log records streamed to Kafka / S3.
// Binary format matches the original CloudLogControllerImpl encoding.
struct CloudWALRecord {
  static const uint32_t kAppend = 0x1;
  static const uint32_t kDelete = 0x2;
  static const uint32_t kClosed = 0x4;

  static void SerializeAppend(const Slice& filename, const Slice& data,
                              uint64_t offset, std::string* out);
  static void SerializeClosed(const Slice& filename, uint64_t file_size,
                              std::string* out);
  static void SerializeDelete(const std::string& filename, std::string* out);
  static bool Extract(const Slice& input, uint32_t* operation,
                      Slice* filename, uint64_t* offset_in_file,
                      uint64_t* file_size, Slice* data);
};

#ifdef USE_KAFKA
class KafkaWALProducer;
#endif

// Wraps a local FSWritableFile and optionally fans out WAL writes to Kafka
// and/or a background S3/GCS uploader.
class CloudWALWritableFile : public FSWritableFile {
 public:
  CloudWALWritableFile(CloudFileSystem* cfs,
                       const std::string& fname,
                       const FileOptions& file_opts,
                       std::unique_ptr<FSWritableFile> local_file,
#ifdef USE_KAFKA
                       KafkaWALProducer* kafka_producer,
#endif
                       const CloudFileSystemOptions& cloud_opts);

  ~CloudWALWritableFile() override;

  using FSWritableFile::Append;
  IOStatus Append(const Slice& data, const IOOptions& opts,
                  IODebugContext* dbg) override;

  IOStatus Flush(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Sync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Fsync(const IOOptions& opts, IODebugContext* dbg) override;
  IOStatus Close(const IOOptions& opts, IODebugContext* dbg) override;

  uint64_t GetFileSize(const IOOptions& opts, IODebugContext* dbg) override;

 private:
  CloudFileSystem* cfs_;
  std::string fname_;
  std::unique_ptr<FSWritableFile> local_file_;  // may be nullptr
#ifdef USE_KAFKA
  KafkaWALProducer* kafka_producer_;  // not owned
#endif
  WalKafkaSyncMode kafka_sync_mode_;
  uint64_t current_offset_;

  // For kPerSync: buffer appended data since last sync for batched publish
  std::string sync_buffer_;
  uint64_t sync_buffer_start_offset_;
};

// Periodically uploads local WAL files to cloud object storage.
class BackgroundWALUploader {
 public:
  BackgroundWALUploader(CloudFileSystem* cfs,
                        const std::string& local_dbname,
                        uint64_t interval_ms);
  ~BackgroundWALUploader();

  void Start();
  void Stop();

  // Upload a specific WAL file immediately (used on Close).
  IOStatus UploadWALFile(const std::string& local_path);

 private:
  void DoUpload(void* arg);

  CloudFileSystem* cfs_;
  std::string local_dbname_;
  uint64_t interval_ms_;
  std::shared_ptr<CloudScheduler> scheduler_;
  long job_handle_;
  std::atomic<bool> running_;
};

// Manages WAL write routing: local files, Kafka publishing, and background
// cloud sync. Constructed by CloudFileSystemImpl when WAL options are set.
class CloudWALController {
 public:
  CloudWALController(CloudFileSystem* cfs,
                     const std::shared_ptr<FileSystem>& base_fs,
                     const CloudFileSystemOptions& opts,
                     const std::shared_ptr<Logger>& logger);
  ~CloudWALController();

  // Create a writable file for a WAL log file.
  IOStatus NewWritableFile(const std::string& fname,
                           const FileOptions& file_opts,
                           std::unique_ptr<FSWritableFile>* result,
                           IODebugContext* dbg);

  // Start background WAL uploader for the given db directory.
  void StartBackgroundUploader(const std::string& local_dbname);

  // Stop background operations and clean up.
  void Stop();

  // Download WAL files from cloud during recovery.
  IOStatus RecoverWALFromCloud(const std::string& local_dbname);

  bool IsActive() const { return active_; }

 private:
  CloudFileSystem* cfs_;
  std::shared_ptr<FileSystem> base_fs_;
  CloudFileSystemOptions cloud_opts_;
  std::shared_ptr<Logger> info_log_;
  bool active_;

#ifdef USE_KAFKA
  std::unique_ptr<KafkaWALProducer> kafka_producer_;
#endif

  std::unique_ptr<BackgroundWALUploader> bg_uploader_;
};

}  // namespace ROCKSDB_NAMESPACE
