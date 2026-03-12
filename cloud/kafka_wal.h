//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.
//
#pragma once

#ifdef USE_KAFKA

#include <memory>
#include <string>

#include "rocksdb/io_status.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"

namespace RdKafka {
class Producer;
class KafkaConsumer;
class Topic;
class Conf;
}  // namespace RdKafka

namespace ROCKSDB_NAMESPACE {

// Publishes serialized WAL records to a Kafka topic.
class KafkaWALProducer {
 public:
  KafkaWALProducer(const std::string& bootstrap_servers,
                   const std::string& topic,
                   const std::shared_ptr<Logger>& logger);
  ~KafkaWALProducer();

  Status Initialize();

  // Publish an Append record for the given filename, data, and file offset.
  IOStatus Publish(const std::string& fname, const Slice& data,
                   uint64_t offset);

  // Publish a Closed record.
  IOStatus PublishClosed(const std::string& fname, uint64_t file_size);

  // Publish a Delete record.
  IOStatus PublishDelete(const std::string& fname);

  // Flush outstanding messages. Blocks until the output queue is drained
  // or timeout_ms elapses.
  IOStatus Flush(int timeout_ms = 10000);

 private:
  IOStatus ProduceRaw(const std::string& key, const std::string& payload);

  std::string bootstrap_servers_;
  std::string topic_name_;
  std::shared_ptr<Logger> info_log_;

  std::unique_ptr<RdKafka::Producer> producer_;
  std::unique_ptr<RdKafka::Topic> topic_;
};

// Consumes WAL records from a Kafka topic and replays them into a local
// cache directory. Used during recovery to reconstruct WAL state.
class KafkaWALTailer {
 public:
  KafkaWALTailer(const std::string& bootstrap_servers,
                 const std::string& topic,
                 const std::string& cache_dir,
                 const std::shared_ptr<FileSystem>& base_fs,
                 const std::shared_ptr<Logger>& logger);
  ~KafkaWALTailer();

  // Consume all available records from the topic and replay them into
  // cache_dir. Returns when caught up to the high-water mark.
  IOStatus ReplayAll();

 private:
  IOStatus Apply(const Slice& data);

  std::string bootstrap_servers_;
  std::string topic_name_;
  std::string cache_dir_;
  std::shared_ptr<FileSystem> base_fs_;
  std::shared_ptr<Logger> info_log_;

  std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
  std::map<std::string, std::unique_ptr<FSRandomRWFile>> cache_fds_;
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_KAFKA
