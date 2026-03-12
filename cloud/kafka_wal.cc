//  Copyright (c) 2024-present, SurrealDB Ltd.  All rights reserved.

#ifdef USE_KAFKA

#include "cloud/kafka_wal.h"

#include <cinttypes>

#include "cloud/cloud_wal_controller.h"
#include "cloud/filename.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"

#include <librdkafka/rdkafkacpp.h>

namespace ROCKSDB_NAMESPACE {

// ---------------------------------------------------------------------------
// KafkaWALProducer
// ---------------------------------------------------------------------------

KafkaWALProducer::KafkaWALProducer(const std::string& bootstrap_servers,
                                   const std::string& topic,
                                   const std::shared_ptr<Logger>& logger)
    : bootstrap_servers_(bootstrap_servers),
      topic_name_(topic),
      info_log_(logger) {}

KafkaWALProducer::~KafkaWALProducer() {
  if (producer_) {
    producer_->flush(5000);
  }
  topic_.reset();
  producer_.reset();
}

Status KafkaWALProducer::Initialize() {
  std::string errstr;
  auto conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (conf->set("bootstrap.servers", bootstrap_servers_, errstr) !=
      RdKafka::Conf::CONF_OK) {
    return Status::InvalidArgument("Kafka config error: " + errstr);
  }

  producer_.reset(RdKafka::Producer::create(conf.get(), errstr));
  if (!producer_) {
    return Status::IOError("Failed to create Kafka producer: " + errstr);
  }

  topic_.reset(RdKafka::Topic::create(producer_.get(), topic_name_,
                                      nullptr, errstr));
  if (!topic_) {
    return Status::IOError("Failed to create Kafka topic: " + errstr);
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[kafka_wal] Producer initialized, brokers=%s topic=%s",
      bootstrap_servers_.c_str(), topic_name_.c_str());
  return Status::OK();
}

IOStatus KafkaWALProducer::ProduceRaw(const std::string& key,
                                      const std::string& payload) {
  auto err = producer_->produce(
      topic_.get(), RdKafka::Topic::PARTITION_UA, RdKafka::Producer::RK_MSG_COPY,
      const_cast<char*>(payload.data()), payload.size(),
      key.data(), key.size(), 0, nullptr);

  if (err != RdKafka::ERR_NO_ERROR) {
    return IOStatus::IOError("Kafka produce failed: " +
                             RdKafka::err2str(err));
  }
  producer_->poll(0);
  return IOStatus::OK();
}

IOStatus KafkaWALProducer::Publish(const std::string& fname, const Slice& data,
                                   uint64_t offset) {
  std::string buffer;
  CloudWALRecord::SerializeAppend(Slice(fname), data, offset, &buffer);
  return ProduceRaw(fname, buffer);
}

IOStatus KafkaWALProducer::PublishClosed(const std::string& fname,
                                         uint64_t file_size) {
  std::string buffer;
  CloudWALRecord::SerializeClosed(Slice(fname), file_size, &buffer);
  return ProduceRaw(fname, buffer);
}

IOStatus KafkaWALProducer::PublishDelete(const std::string& fname) {
  std::string buffer;
  CloudWALRecord::SerializeDelete(fname, &buffer);
  return ProduceRaw(fname, buffer);
}

IOStatus KafkaWALProducer::Flush(int timeout_ms) {
  auto err = producer_->flush(timeout_ms);
  if (err != RdKafka::ERR_NO_ERROR) {
    if (err == RdKafka::ERR__TIMED_OUT) {
      return IOStatus::TimedOut("Kafka flush timed out");
    }
    return IOStatus::IOError("Kafka flush failed: " + RdKafka::err2str(err));
  }
  return IOStatus::OK();
}

// ---------------------------------------------------------------------------
// KafkaWALTailer
// ---------------------------------------------------------------------------

KafkaWALTailer::KafkaWALTailer(const std::string& bootstrap_servers,
                               const std::string& topic,
                               const std::string& cache_dir,
                               const std::shared_ptr<FileSystem>& base_fs,
                               const std::shared_ptr<Logger>& logger)
    : bootstrap_servers_(bootstrap_servers),
      topic_name_(topic),
      cache_dir_(cache_dir),
      base_fs_(base_fs),
      info_log_(logger) {}

KafkaWALTailer::~KafkaWALTailer() {
  cache_fds_.clear();
  if (consumer_) {
    consumer_->close();
  }
}

IOStatus KafkaWALTailer::Apply(const Slice& data) {
  uint32_t operation;
  Slice filename, payload;
  uint64_t offset_in_file, file_size;

  if (!CloudWALRecord::Extract(data, &operation, &filename, &offset_in_file,
                               &file_size, &payload)) {
    return IOStatus::Corruption("Failed to parse WAL record from Kafka");
  }

  std::string fname = filename.ToString();
  std::string cache_path = cache_dir_ + "/" + basename(fname);

  if (operation == CloudWALRecord::kAppend) {
    auto it = cache_fds_.find(cache_path);
    if (it == cache_fds_.end()) {
      std::unique_ptr<FSRandomRWFile> f;
      auto s = base_fs_->NewRandomRWFile(cache_path, FileOptions(), &f,
                                         nullptr);
      if (!s.ok()) return s;
      cache_fds_[cache_path] = std::move(f);
      it = cache_fds_.find(cache_path);
    }
    return it->second->Write(offset_in_file, payload, IOOptions(), nullptr);
  } else if (operation == CloudWALRecord::kClosed) {
    cache_fds_.erase(cache_path);
  } else if (operation == CloudWALRecord::kDelete) {
    cache_fds_.erase(cache_path);
    base_fs_->DeleteFile(cache_path, IOOptions(), nullptr);
  }

  return IOStatus::OK();
}

IOStatus KafkaWALTailer::ReplayAll() {
  std::string errstr;
  auto conf = std::unique_ptr<RdKafka::Conf>(
      RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  conf->set("bootstrap.servers", bootstrap_servers_, errstr);
  conf->set("group.id", "rocksdb-wal-tailer-" + topic_name_, errstr);
  conf->set("auto.offset.reset", "earliest", errstr);
  conf->set("enable.auto.commit", "false", errstr);

  consumer_.reset(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!consumer_) {
    return IOStatus::IOError("Failed to create Kafka consumer: " + errstr);
  }

  auto err = consumer_->subscribe({topic_name_});
  if (err != RdKafka::ERR_NO_ERROR) {
    return IOStatus::IOError("Kafka subscribe failed: " +
                             RdKafka::err2str(err));
  }

  // Create cache directory
  base_fs_->CreateDirIfMissing(cache_dir_, IOOptions(), nullptr);

  int consecutive_empty = 0;
  while (consecutive_empty < 3) {
    auto msg = std::unique_ptr<RdKafka::Message>(consumer_->consume(1000));
    if (!msg) {
      consecutive_empty++;
      continue;
    }
    if (msg->err() == RdKafka::ERR_NO_ERROR) {
      consecutive_empty = 0;
      Slice payload(static_cast<const char*>(msg->payload()), msg->len());
      auto s = Apply(payload);
      if (!s.ok()) {
        Log(InfoLogLevel::WARN_LEVEL, info_log_,
            "[kafka_wal] Failed to apply Kafka record: %s",
            s.ToString().c_str());
      }
    } else if (msg->err() == RdKafka::ERR__PARTITION_EOF) {
      consecutive_empty++;
    } else if (msg->err() == RdKafka::ERR__TIMED_OUT) {
      consecutive_empty++;
    } else {
      return IOStatus::IOError("Kafka consume error: " + msg->errstr());
    }
  }

  consumer_->close();
  consumer_.reset();

  Log(InfoLogLevel::INFO_LEVEL, info_log_,
      "[kafka_wal] Replay complete for topic %s into %s",
      topic_name_.c_str(), cache_dir_.c_str());
  return IOStatus::OK();
}

}  // namespace ROCKSDB_NAMESPACE

#endif  // USE_KAFKA
