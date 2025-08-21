/**
 *    Copyright (C) 2025 EloqData Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under either of the following two licenses:
 *    1. GNU Affero General Public License, version 3, as published by the Free
 *    Software Foundation.
 *    2. GNU General Public License as published by the Free Software
 *    Foundation; version 2 of the License.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License or GNU General Public License for more
 *    details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    and GNU General Public License V2 along with this program.  If not, see
 *    <http://www.gnu.org/licenses/>.
 *
 */
#pragma once

#include <cstdint>
#ifdef USE_ROCKSDB_LOG_STATE

#include <google/protobuf/message_lite.h>
#include <rocksdb/db.h>
#include <rocksdb/listener.h>

#include <memory>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bthread/condition_variable.h"
#include "bthread/mutex.h"

#include "log_state.h"

namespace txlog
{
// commit_ts(8)----tx_number(8)
inline void Serialize(std::array<char, 16> &res,
                      uint64_t timestamp,
                      uint64_t tx_number)
{
    char *p = res.data();
    uint64_t ts_be = __builtin_bswap64(timestamp);
    std::memcpy(p, &ts_be, sizeof(uint64_t));

    p += sizeof(uint64_t);

    uint64_t tx_no_be = __builtin_bswap64(tx_number);
    std::memcpy(p, &tx_no_be, sizeof(uint64_t));
}

// tx_number(8)----commit_ts(8)----code(1)
inline void Serialize(std::array<char, 17> &res,
                      uint64_t timestamp,
                      uint64_t tx_number,
                      uint8_t code)
{
    char *p = res.data();

    uint64_t tx_no_be = __builtin_bswap64(tx_number);
    std::memcpy(p, &tx_no_be, sizeof(uint64_t));
    p += sizeof(uint64_t);

    uint64_t ts_be = __builtin_bswap64(timestamp);
    std::memcpy(p, &ts_be, sizeof(uint64_t));
    p += sizeof(uint64_t);

    res[res.size() - 1] = (char) code;
}

inline void Deserialize(rocksdb::Slice key,
                        uint64_t &timestamp,
                        uint64_t &tx_number)
{
    assert(key.size() == 16);
    const char *p = key.data();
    uint64_t ts_be, tx_no_be;
    std::memcpy(&ts_be, p, sizeof(uint64_t));
    timestamp = __builtin_bswap64(ts_be);
    p += sizeof(uint64_t);

    std::memcpy(&tx_no_be, p, sizeof(uint64_t));
    tx_number = __builtin_bswap64(tx_no_be);
}

inline void Deserialize(
    rocksdb::Slice key,
    rocksdb::Slice value,
    uint64_t &timestamp,
    uint64_t &tx_number,
    ::google::protobuf::RepeatedPtrField<SchemaOpMessage> &schemas_op)
{
    assert(key.size() == 17);
    const char *p = key.data();
    uint64_t ts_be, tx_no_be;

    std::memcpy(&tx_no_be, p, sizeof(uint64_t));
    tx_number = __builtin_bswap64(tx_no_be);
    p += sizeof(uint64_t);

    std::memcpy(&ts_be, p, sizeof(uint64_t));
    timestamp = __builtin_bswap64(ts_be);
    p += sizeof(uint64_t);

    const char *ptr = value.data();
    uint16_t cnt = *reinterpret_cast<const uint16_t *>(ptr);
    ptr += sizeof(cnt);
    for (uint16_t idx = 0; idx < cnt; ++idx)
    {
        uint32_t len = *reinterpret_cast<const uint32_t *>(ptr);
        ptr += sizeof(len);
        SchemaOpMessage *schema_op_msg = schemas_op.Add();
        schema_op_msg->ParseFromArray(ptr, len);
        ptr += len;
    }
}

inline void Deserialize(rocksdb::Slice key,
                        rocksdb::Slice value,
                        uint64_t &timestamp,
                        uint64_t &tx_number,
                        SplitRangeOpMessage &range_op)
{
    assert(key.size() == 17);
    const char *p = key.data();
    uint64_t ts_be, tx_no_be;

    std::memcpy(&tx_no_be, p, sizeof(uint64_t));
    tx_number = __builtin_bswap64(tx_no_be);
    p += sizeof(uint64_t);

    std::memcpy(&ts_be, p, sizeof(uint64_t));
    timestamp = __builtin_bswap64(ts_be);
    p += sizeof(uint64_t);

    range_op.ParseFromString(value.ToString());
}

class ItemIteratorRocksDBImpl : public ItemIterator
{
public:
    explicit ItemIteratorRocksDBImpl(size_t worker_num,
                                     std::vector<Item::Pointer> &&ddl_list,
                                     rocksdb::DB *db,
                                     uint64_t start_ts)
        : ItemIterator(std::move(ddl_list)),
          db_(db),
          start_key_(start_key_storage_.data(), start_key_storage_.size()),
          worker_num_(worker_num)
    {
        Serialize(start_key_storage_, start_ts, 0);
        rocksdb::ReadOptions read_options;
        // set iterate_lower_bound for read_options for better performance
        read_options.iterate_lower_bound = &start_key_;
        //        read_options.readahead_size = 4 << 20;
        rocksdb_iterator_ =
            std::unique_ptr<rocksdb::Iterator>(db_->NewIterator(read_options));
        uint64_t first_ts = 0;
        uint64_t last_ts = 0;
        uint64_t tmp_txn;
        rocksdb_iterator_->SeekToFirst();
        if (!rocksdb_iterator_->Valid())
        {
            return;
        }
        Deserialize(rocksdb_iterator_->key(), first_ts, tmp_txn);
        rocksdb_iterator_->SeekToLast();
        if (!rocksdb_iterator_->Valid())
        {
            return;
        }
        keys_storage_.reserve(worker_num_ * 2);
        keys_.reserve(worker_num_ * 2);
        Deserialize(rocksdb_iterator_->key(), last_ts, tmp_txn);
        std::vector<uint64_t> ts_list;
        ts_list.push_back(first_ts);
        auto gap = (last_ts - first_ts) / worker_num_;
        for (size_t i = 1; i < worker_num_; i++)
        {
            ts_list.push_back(first_ts + gap * i);
        }
        ts_list.push_back(last_ts);
        for (size_t i = 0; i < ts_list.size() - 1; i++)
        {
            rocksdb::ReadOptions read_options;
            std::array<char, 16> start_key{};
            // range start: current_ts, target_ng, txn number 0
            Serialize(start_key, ts_list[i], 0);
            keys_storage_.push_back(start_key);
            keys_.push_back(rocksdb::Slice(keys_storage_.back().data(),
                                           keys_storage_.back().size()));
            read_options.iterate_lower_bound = &keys_.back();

            if (i != ts_list.size() - 2)
            {
                std::array<char, 16> end_key{};
                // range end: next_ts - 1, target_ng, txn number UINT64_MAX
                Serialize(end_key, ts_list[i + 1] - 1, UINT64_MAX);
                keys_storage_.push_back(end_key);
                keys_.push_back(rocksdb::Slice(keys_storage_.back().data(),
                                               keys_storage_.back().size()));
                read_options.iterate_upper_bound = &keys_.back();
            }
            rocksdb_iterators_.push_back(std::unique_ptr<rocksdb::Iterator>(
                db_->NewIterator(read_options)));
        }
        items_.resize(worker_num_);
    }

    ~ItemIteratorRocksDBImpl() override = default;

    void SeekToFirst() override
    {
        ddl_idx_ = 0;
        rocksdb_iterator_->SeekToFirst();
    }

    bool Valid() override
    {
        if (ddl_idx_ < ddl_list_.size() || rocksdb_iterator_->Valid())
        {
            return true;
        }
        if (!rocksdb_iterator_->status().ok())
        {
            LOG(ERROR) << "RocksDB iterator failed: "
                       << rocksdb_iterator_->status().ToString();
        }
        LOG(INFO) << "total get item from rocksdb iterator: " << total;
        return false;
    };

    void Next() override
    {
        if (ddl_idx_ < ddl_list_.size())
        {
            ddl_idx_++;
        }
        else
        {
            rocksdb_iterator_->Next();
        }
    };

    const Item &GetItem() override
    {
        total++;
        if (ddl_idx_ < ddl_list_.size())
        {
            return *ddl_list_.at(ddl_idx_);
        }
        uint64_t timestamp, tx_number;
        Deserialize(rocksdb_iterator_->key(), timestamp, tx_number);
        rocksdb::Slice value = rocksdb_iterator_->value();
        item_.tx_number_ = tx_number;
        item_.timestamp_ = timestamp;
        item_.log_message_ = {value.data(), value.size()};
        item_.item_type_ = LogItemType::DataLog;
        return item_;
    };

    size_t IteratorNum() override
    {
        return rocksdb_iterators_.size();
    }

    void SeekToFirst(size_t idx) override
    {
        rocksdb_iterators_[idx]->SeekToFirst();
    }

    bool Valid(size_t idx) override
    {
        if (rocksdb_iterators_[idx]->Valid())
        {
            return true;
        }
        if (!rocksdb_iterators_[idx]->status().ok())
        {
            LOG(ERROR) << "RocksDB iterator failed: "
                       << rocksdb_iterators_[idx]->status().ToString();
        }
        return false;
    }

    void Next(size_t idx) override
    {
        rocksdb_iterators_[idx]->Next();
    }

    const Item &GetItem(size_t idx) override
    {
        uint64_t timestamp, tx_number;
        Deserialize(rocksdb_iterators_[idx]->key(), timestamp, tx_number);
        rocksdb::Slice value = rocksdb_iterators_[idx]->value();
        items_[idx].tx_number_ = tx_number;
        items_[idx].timestamp_ = timestamp;
        items_[idx].log_message_ = {value.data(), value.size()};
        items_[idx].item_type_ = LogItemType::DataLog;
        return items_[idx];
    };

    void SeekToDDLFirst() override
    {
        ddl_idx_ = 0;
    }

    bool ValidDDL() override
    {
        return ddl_idx_ < ddl_list_.size();
    }

    void NextDDL() override
    {
        ddl_idx_++;
    }

    const Item &GetDDLItem() override
    {
        return *ddl_list_.at(ddl_idx_);
    }

private:
    int total{};
    rocksdb::DB *db_;
    std::array<char, 16> start_key_storage_;
    rocksdb::Slice start_key_;
    std::unique_ptr<rocksdb::Iterator> rocksdb_iterator_;
    Item item_;
    std::vector<std::array<char, 16>> keys_storage_;
    std::vector<rocksdb::Slice> keys_;
    std::vector<std::unique_ptr<rocksdb::Iterator>> rocksdb_iterators_;
    std::vector<Item> items_;
    size_t worker_num_{1};
};

class LogStateRocksDBImpl : public LogState
{
public:
    explicit LogStateRocksDBImpl(std::string rocksdb_path,
                                 const size_t sst_files_size_limit,
                                 const size_t rocksdb_scan_threads);

    ~LogStateRocksDBImpl() override;

    int AddLogItem(uint64_t tx_number,
                   uint64_t timestamp,
                   const std::string &log_message) override;

    int AddLogItemBatch(
        const std::vector<std::tuple<uint64_t, uint64_t, std::string>>
            &batch_logs);

    std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint64_t start_timestamp) override;

    std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number, uint64_t lower_bound_ts = 0) override;

    /**
     * Start will be called in starting LogState.
     */
    int Start() override;

    /**
     * DB purge thread function
     */
    void PurgingSstFiles();
    void NotifySstFileCreated(const rocksdb::FlushJobInfo &flush_job_info);
    uint64_t GetSstFilesSize();

    uint64_t GetApproximateReplayLogSize() override;

    inline static std::string meta_family = "meta_cf";

private:
    int PersistSchemaOp(uint64_t txn,
                        uint64_t timestamp,
                        const std::string &schema_op_str) override;

    int PersistSchemasOp(
        uint64_t txn,
        uint64_t timestamp,
        const ::google::protobuf::RepeatedPtrField<SchemaOpMessage> &schemas_op)
        override;

    int DeleteSchemaOp(uint64_t txn, uint64_t timestamp) override;

    int PersistRangeOp(uint64_t txn,
                       uint64_t timestamp,
                       const std::string &range_op_str) override;

    int DeleteRangeOp(uint64_t txn, uint64_t timestamp) override;

    int PersistCkptAndMaxTxn(uint64_t ckpt_ts, uint32_t max_txn) override;

    void StopRocksDB();

    static void PrintKey(rocksdb::Slice key);

    /**
     * close db_ and remove all files and dirs in rocksdb storage path but keep
     * the directory itself
     */
    void CloseAndClearRocksDB();

    /**
     * Stores log records for cc node groups.
     * Committed log records in the log state machine are organized by cc node
     * groups and within one group are ordered by commit timestamps.
     * This organization facilitates log truncation but is less efficient when
     * looking for the log record committed by a specific tx.
     */
    rocksdb::DB *db_;
    rocksdb::ColumnFamilyHandle *default_handle_{nullptr};
    rocksdb::ColumnFamilyHandle *meta_handle_{nullptr};
    std::string rocksdb_storage_path_;
    rocksdb::WriteOptions write_option_;

    /**
     * Last truncate timestamp of every ng_id on this log group.
     */
    std::unordered_map<uint32_t, uint64_t> truncate_ts_;

    /**
     * The size of sst files in rocksdb storage path.
     */
    uint64_t sst_files_size_{0};
    /**
     * The max threshold of sst files size in rocksdb storage path.
     * If the size of sst files exceeds this threshold, the purge thread will
     * be triggered.
     */
    const size_t sst_files_size_limit_{0};

    const size_t rocksdb_scan_threads_{1};
    /**
     * The thread for dynamically calculating the size of sst files, and purge
     * sst files
     */
    std::thread purge_thread_;
    std::queue<rocksdb::FlushJobInfo> sst_created_queue_;
    bthread::Mutex sst_queue_mutex_;
    bthread::ConditionVariable sst_queue_cv_;
    uint64_t last_purging_sst_ckpt_ts_;
    std::atomic<bool> stop_purge_thread_{false};
};

class RocksDBEventListener : public rocksdb::EventListener
{
public:
    explicit RocksDBEventListener(LogStateRocksDBImpl *log_state)
        : log_state_(log_state)
    {
    }
    /**
     * Monitor the SST files creation
     */
    void OnFlushCompleted(rocksdb::DB *db,
                          const rocksdb::FlushJobInfo &flush_job_info) override;

private:
    std::string GetFlushReason(rocksdb::FlushReason flush_reason);
    LogStateRocksDBImpl *log_state_;
};
}  // namespace txlog
#endif
