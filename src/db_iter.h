#pragma once

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <memory>
#include <unordered_map>

#include "db/db_iter.h"
#include "logging/logging.h"
#include "rocksdb/env.h"

#include "titan_stats.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(const TitanReadOptions& options, BlobStorage* storage,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter, Env* env,
                  TitanStats* stats, Logger* info_log)
      : options_(options),
        storage_(storage),
        snap_(snap),
        iter_(std::move(iter)),
        env_(env),
        stats_(stats),
        info_log_(info_log) {}

  bool Valid() const override { return iter_->Valid() && status_.ok(); }

  Status status() const override {
    // assume volatile inner iter
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    if (ShouldGetBlobValue()) {
      StopWatch seek_sw(env_, stats_, BLOB_DB_SEEK_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_SEEK);
    }
  }

  void Next() override {
    assert(Valid());
    iter_->Next();
    if (ShouldGetBlobValue()) {
      StopWatch next_sw(env_, stats_, BLOB_DB_NEXT_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_NEXT);
    }
  }

  void Prev() override {
    assert(Valid());
    iter_->Prev();
    if (ShouldGetBlobValue()) {
      StopWatch prev_sw(env_, stats_, BLOB_DB_PREV_MICROS);
      GetBlobValue();
      RecordTick(stats_, BLOB_DB_NUM_PREV);
    }
  }

  Slice key() const override {
    assert(Valid());
    return iter_->key();
  }

  Slice value() const override {
    assert(Valid() && !options_.key_only);
    if (options_.key_only) return Slice();
    if (!iter_->IsBlob()) return iter_->value();
    return record_.value;
  }

 private:
  bool ShouldGetBlobValue() {
    if (!iter_->Valid() || !iter_->IsBlob() || options_.key_only) {
      status_ = iter_->status();
      return false;
    }
    return true;
  }

  void GetBlobValue() {
    // TODO: cache here
    assert(iter_->status().ok());

    BlobIndex index;
    status_ = DecodeInto(iter_->value(), &index);
    if (!status_.ok()) {
      ROCKS_LOG_ERROR(info_log_,
                      "Titan iterator: failed to decode blob index %s: %s",
                      iter_->value().ToString(true /*hex*/).c_str(),
                      status_.ToString().c_str());
      return;
    }

    // TODO: iterator 命中cache概率应该比较小
    std::string cache_key;
    Cache::Handle* cache_handle = nullptr;
    auto blob_cache_ = storage_->cf_options().blob_cache.get();

    if (blob_cache_ != nullptr) {
      cache_key.assign(cache_prefix_);
      index.EncodeTo(&cache_key);
      cache_handle = blob_cache_->Lookup(cache_key);
      if (cache_handle != nullptr) {
        auto* cached_blob =
            reinterpret_cast<OwnedSlice*>(blob_cache_->Value(cache_handle));
        // 相当于借用cached_blob的内存, 在析构的时候只能调用UnrefCacheHandle
        buffer_.Reset();
        buffer_.PinSlice(*cached_blob, UnrefCacheHandle, blob_cache_,
                         cache_handle);
        status_ = DecodeInto(*cached_blob, &record_);
        if (!status_.ok())  {
          return;
        }
        return;
      }
    }


    // 没有使用和blob_storage一致的Get接口，导致这里需要特殊处理一下
    auto it = files_.find(index.file_number);
    if (it == files_.end()) {
      std::unique_ptr<BlobFilePrefetcher> prefetcher;
      status_ = storage_->NewPrefetcher(index.file_number, &prefetcher);
      if (!status_.ok()) {
        ROCKS_LOG_ERROR(
            info_log_,
            "Titan iterator: failed to create prefetcher for blob file %" PRIu64
            ": %s",
            index.file_number, status_.ToString().c_str());
        return;
      }
      it = files_.emplace(index.file_number, std::move(prefetcher)).first;
    }

    buffer_.Reset();
    status_ = it->second->Get(options_, index.blob_handle, &record_, &buffer_);
    if (!status_.ok()) {
      ROCKS_LOG_ERROR(
          info_log_,
          "Titan iterator: failed to read blob value from file %" PRIu64
          ", offset %" PRIu64 ", size %" PRIu64 ": %s\n",
          index.file_number, index.blob_handle.offset, index.blob_handle.size,
          status_.ToString().c_str());
    }
    return;
  }

  Status status_;
  BlobRecord record_;
  PinnableSlice buffer_;

  TitanReadOptions options_;
  BlobStorage* storage_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  std::unordered_map<uint64_t, std::unique_ptr<BlobFilePrefetcher>> files_;

  Env* env_;
  TitanStats* stats_;
  Logger* info_log_;
  const std::string cache_prefix_;
};

}  // namespace titandb
}  // namespace rocksdb
