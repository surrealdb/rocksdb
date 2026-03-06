//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "utilities/transactions/optimistic_transaction.h"

#include <cstdint>
#include <string>

#include "db/column_family.h"
#include "db/db_impl/db_impl.h"
#include "db/write_batch_internal.h"
#include "rocksdb/comparator.h"
#include "rocksdb/db.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/write_batch_with_index.h"
#include "utilities/write_batch_with_index/write_batch_with_index_internal.h"
#include "util/cast_util.h"
#include "util/coding.h"
#include "util/defer.h"
#include "util/string_util.h"
#include "utilities/transactions/lock/point/point_lock_tracker.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"
#include "utilities/transactions/transaction_util.h"

namespace ROCKSDB_NAMESPACE {

struct WriteOptions;

OptimisticTransaction::OptimisticTransaction(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options)
    : TransactionBaseImpl(txn_db->GetBaseDB(), write_options,
                          PointLockTrackerFactory::Get()),
      txn_db_(txn_db) {
  Initialize(txn_options);
}

void OptimisticTransaction::Initialize(
    const OptimisticTransactionOptions& txn_options) {
  if (txn_options.set_snapshot) {
    SetSnapshot();
  }
}

void OptimisticTransaction::Reinitialize(
    OptimisticTransactionDB* txn_db, const WriteOptions& write_options,
    const OptimisticTransactionOptions& txn_options) {
  TransactionBaseImpl::Reinitialize(txn_db->GetBaseDB(), write_options);
  Initialize(txn_options);
}

OptimisticTransaction::~OptimisticTransaction() = default;

void OptimisticTransaction::Clear() {
  TransactionBaseImpl::Clear();
  read_timestamp_ = kMaxTxnTimestamp;
  commit_timestamp_ = kMaxTxnTimestamp;
  cfs_with_ts_tracked_when_indexing_disabled_.clear();
}

Status OptimisticTransaction::Prepare() {
  return Status::InvalidArgument(
      "Two phase commit not supported for optimistic transactions.");
}

Status OptimisticTransaction::Commit() {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  switch (txn_db_impl->GetValidatePolicy()) {
    case OccValidationPolicy::kValidateParallel:
      return CommitWithParallelValidate();
    case OccValidationPolicy::kValidateSerial:
      return CommitWithSerialValidate();
    default:
      assert(0);
  }
  // unreachable, just void compiler complain
  return Status::OK();
}

Status OptimisticTransaction::SetReadTimestampForValidation(TxnTimestamp ts) {
  if (read_timestamp_ < kMaxTxnTimestamp && ts < read_timestamp_) {
    return Status::InvalidArgument(
        "Cannot decrease read timestamp for validation");
  }
  read_timestamp_ = ts;
  return Status::OK();
}

Status OptimisticTransaction::SetCommitTimestamp(TxnTimestamp ts) {
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  if (txn_db_impl->GetEnableUdtValidation() &&
      read_timestamp_ < kMaxTxnTimestamp && ts <= read_timestamp_) {
    return Status::InvalidArgument(
        "Cannot commit at timestamp smaller than or equal to read timestamp");
  }
  commit_timestamp_ = ts;
  return Status::OK();
}

template <typename TKey, typename TOperation>
Status OptimisticTransaction::Operate(ColumnFamilyHandle* column_family,
                                      const TKey& key, const bool do_validate,
                                      const bool assume_tracked,
                                      TOperation&& operation) {
  Status s;
  if constexpr (std::is_same_v<Slice, TKey>) {
    s = TryLock(column_family, key, /*read_only=*/false, /*exclusive=*/true,
                do_validate, assume_tracked);
  } else if constexpr (std::is_same_v<SliceParts, TKey>) {
    std::string key_buf;
    Slice contiguous_key(key, &key_buf);
    s = TryLock(column_family, contiguous_key, /*read_only=*/false,
                /*exclusive=*/true, do_validate, assume_tracked);
  }
  if (!s.ok()) {
    return s;
  }
  column_family = column_family ? column_family
                                : db_->DefaultColumnFamily();
  assert(column_family);
  const Comparator* const ucmp = column_family->GetComparator();
  assert(ucmp);
  size_t ts_sz = ucmp->timestamp_size();
  if (ts_sz > 0) {
    assert(ts_sz == sizeof(TxnTimestamp));
    if (!IndexingEnabled()) {
      cfs_with_ts_tracked_when_indexing_disabled_.insert(
          column_family->GetID());
    }
  }
  return operation();
}

Status OptimisticTransaction::Put(ColumnFamilyHandle* column_family,
                                  const Slice& key, const Slice& value,
                                  const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->Put(column_family, key, value);
  });
}

Status OptimisticTransaction::Put(ColumnFamilyHandle* column_family,
                                  const SliceParts& key,
                                  const SliceParts& value,
                                  const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->Put(column_family, key, value);
  });
}

Status OptimisticTransaction::Delete(ColumnFamilyHandle* column_family,
                                     const Slice& key,
                                     const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->Delete(column_family, key);
  });
}

Status OptimisticTransaction::Delete(ColumnFamilyHandle* column_family,
                                     const SliceParts& key,
                                     const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->Delete(column_family, key);
  });
}

Status OptimisticTransaction::SingleDelete(ColumnFamilyHandle* column_family,
                                           const Slice& key,
                                           const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->SingleDelete(column_family, key);
  });
}

Status OptimisticTransaction::SingleDelete(ColumnFamilyHandle* column_family,
                                           const SliceParts& key,
                                           const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->SingleDelete(column_family, key);
  });
}

Status OptimisticTransaction::Merge(ColumnFamilyHandle* column_family,
                                    const Slice& key, const Slice& value,
                                    const bool assume_tracked) {
  const bool do_validate = !assume_tracked;
  return Operate(column_family, key, do_validate, assume_tracked, [&]() {
    return GetBatchForWrite()->Merge(column_family, key, value);
  });
}

Status OptimisticTransaction::MaybeStampWriteBatchTimestamps() {
  WriteBatchWithIndex* wbwi = GetWriteBatch();
  assert(wbwi);
  WriteBatch* wb = wbwi->GetWriteBatch();
  assert(wb);

  const bool needs_ts = WriteBatchInternal::HasKeyWithTimestamp(*wb);
  if (!needs_ts) {
    return Status::OK();
  }
  if (commit_timestamp_ == kMaxTxnTimestamp) {
    return Status::InvalidArgument("Must assign a commit timestamp");
  }

  char commit_ts_buf[sizeof(kMaxTxnTimestamp)];
  EncodeFixed64(commit_ts_buf, commit_timestamp_);
  Slice commit_ts(commit_ts_buf, sizeof(commit_ts_buf));

  return wb->UpdateTimestamps(
      commit_ts, [wb, wbwi, this](uint32_t cf) -> size_t {
        auto cf_id_to_ts_sz = wb->GetColumnFamilyToTimestampSize();
        auto iter = cf_id_to_ts_sz.find(cf);
        if (iter != cf_id_to_ts_sz.end()) {
          return iter->second;
        }
        auto cf_iter = cfs_with_ts_tracked_when_indexing_disabled_.find(cf);
        if (cf_iter != cfs_with_ts_tracked_when_indexing_disabled_.end()) {
          return sizeof(kMaxTxnTimestamp);
        }
        const Comparator* ucmp =
            WriteBatchWithIndexInternal::GetUserComparator(*wbwi, cf);
        return ucmp ? ucmp->timestamp_size()
                    : std::numeric_limits<size_t>::max();
      });
}

Status OptimisticTransaction::CommitWithSerialValidate() {
  Status s = MaybeStampWriteBatchTimestamps();
  if (!s.ok()) {
    return s;
  }

  // Set up callback which will call CheckTransactionForConflicts() to
  // check whether this transaction is safe to be committed.
  OptimisticTransactionCallback callback(this);

  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());

  s = db_impl->WriteWithCallback(
      write_options_, GetWriteBatch()->GetWriteBatch(), &callback);

  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::CommitWithParallelValidate() {
  Status s = MaybeStampWriteBatchTimestamps();
  if (!s.ok()) {
    return s;
  }

  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);
  assert(txn_db_impl);
  DBImpl* db_impl = static_cast_with_check<DBImpl>(db_->GetRootDB());
  assert(db_impl);
  std::set<port::Mutex*> lk_ptrs;
  std::unique_ptr<LockTracker::ColumnFamilyIterator> cf_it(
      tracked_locks_->GetColumnFamilyIterator());
  assert(cf_it != nullptr);
  while (cf_it->HasNext()) {
    ColumnFamilyId cf = cf_it->Next();

    // To avoid the same key(s) contending across CFs or DBs, seed the
    // hash independently.
    uint64_t seed = reinterpret_cast<uintptr_t>(db_impl) +
                    uint64_t{0xb83c07fbc6ced699} /*random prime*/ * cf;

    std::unique_ptr<LockTracker::KeyIterator> key_it(
        tracked_locks_->GetKeyIterator(cf));
    assert(key_it != nullptr);
    while (key_it->HasNext()) {
      auto lock_bucket_ptr = &txn_db_impl->GetLockBucket(key_it->Next(), seed);
      TEST_SYNC_POINT_CALLBACK(
          "OptimisticTransaction::CommitWithParallelValidate::lock_bucket_ptr",
          lock_bucket_ptr);
      lk_ptrs.insert(lock_bucket_ptr);
    }
  }
  // NOTE: in a single txn, all bucket-locks are taken in ascending order.
  // In this way, txns from different threads all obey this rule so that
  // deadlock can be avoided.
  for (auto v : lk_ptrs) {
    // WART: if an exception is thrown during a Lock(), previously locked will
    // not be Unlock()ed. But a vector of MutexLock is likely inefficient.
    v->Lock();
  }
  Defer unlocks([&]() {
    for (auto v : lk_ptrs) {
      v->Unlock();
    }
  });

  s = TransactionUtil::CheckKeysForConflicts(
      db_impl, *tracked_locks_, true /* cache_only */, read_timestamp_,
      txn_db_impl->GetEnableUdtValidation());
  if (!s.ok()) {
    return s;
  }

  s = db_impl->Write(write_options_, GetWriteBatch()->GetWriteBatch());
  if (s.ok()) {
    Clear();
  }

  return s;
}

Status OptimisticTransaction::Rollback() {
  Clear();
  return Status::OK();
}

// Record this key so that we can check it for conflicts at commit time.
//
// 'exclusive' is unused for OptimisticTransaction.
Status OptimisticTransaction::TryLock(ColumnFamilyHandle* column_family,
                                      const Slice& key, bool read_only,
                                      bool exclusive, const bool do_validate,
                                      const bool assume_tracked) {
  assert(!assume_tracked);  // not supported
  (void)assume_tracked;
  if (!do_validate) {
    return Status::OK();
  }
  uint32_t cfh_id = GetColumnFamilyID(column_family);

  SetSnapshotIfNeeded();

  SequenceNumber seq;
  if (snapshot_) {
    seq = snapshot_->GetSequenceNumber();
  } else {
    seq = db_->GetLatestSequenceNumber();
  }

  std::string key_str = key.ToString();

  TrackKey(cfh_id, key_str, seq, read_only, exclusive);

  // Always return OK. Confilct checking will happen at commit time.
  return Status::OK();
}

// Returns OK if it is safe to commit this transaction.  Returns Status::Busy
// if there are read or write conflicts that would prevent us from committing OR
// if we can not determine whether there would be any such conflicts.
//
// Should only be called on writer thread in order to avoid any race conditions
// in detecting write conflicts.
Status OptimisticTransaction::CheckTransactionForConflicts(DB* db) {
  auto db_impl = static_cast_with_check<DBImpl>(db);
  auto txn_db_impl = static_cast_with_check<OptimisticTransactionDBImpl,
                                            OptimisticTransactionDB>(txn_db_);

  // Since we are on the write thread and do not want to block other writers,
  // we will do a cache-only conflict check.  This can result in TryAgain
  // getting returned if there is not sufficient memtable history to check
  // for conflicts.
  return TransactionUtil::CheckKeysForConflicts(
      db_impl, *tracked_locks_, true /* cache_only */, read_timestamp_,
      txn_db_impl->GetEnableUdtValidation());
}

Status OptimisticTransaction::SetName(const TransactionName& /* unused */) {
  return Status::InvalidArgument("Optimistic transactions cannot be named.");
}

}  // namespace ROCKSDB_NAMESPACE
