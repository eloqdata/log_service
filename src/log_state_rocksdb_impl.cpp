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
#include <rocksdb/db.h>
#include <rocksdb/statistics.h>

#include <cstdint>
#include <string>

#include <rocksdb/sst_file_reader.h>
#include <rocksdb/utilities/checkpoint.h>

#include <filesystem>
#include <iomanip>
#include <memory>
#include <system_error>

#include "log_state_rocksdb_impl.h"
#include "log_utils.h"
#include "rocksdb/convenience.h"

namespace txlog
{
LogStateRocksDBImpl::LogStateRocksDBImpl(std::string rocksdb_path,
                                         const size_t sst_files_size_limit,
                                         const size_t rocksdb_scan_threads)
    : db_(nullptr),
      rocksdb_storage_path_(std::move(rocksdb_path)),
      sst_files_size_limit_(sst_files_size_limit),
      rocksdb_scan_threads_(rocksdb_scan_threads),
      last_purging_sst_ckpt_ts_(0){};

LogStateRocksDBImpl::~LogStateRocksDBImpl()
{
    StopRocksDB();
}

int LogStateRocksDBImpl::AddLogItemBatch(
    const std::vector<std::tuple<uint64_t, uint64_t, std::string>> &batch_logs)
{
    rocksdb::WriteBatch batch;

    for (const auto &[tx_number, timestamp, log_message] : batch_logs)
    {
        std::array<char, 16> key{};
        Serialize(key, timestamp, tx_number);
        batch.Put(rocksdb::Slice(key.data(), key.size()), log_message);
    }

    rocksdb::Status status = db_->Write(write_option_, &batch);
    if (!status.ok())
    {
        LOG(ERROR) << "batch add log items failed: " << status.ToString()
                   << ", RocksDB error code: " << (int) status.code()
                   << ", batch size: " << batch_logs.size();
    }

    return (int) status.code();
}

int LogStateRocksDBImpl::AddLogItem(uint64_t tx_number,
                                    uint64_t timestamp,
                                    const std::string &log_message)
{
    std::array<char, 16> key{};
    Serialize(key, timestamp, tx_number);
    rocksdb::Status status = db_->Put(
        write_option_, rocksdb::Slice(key.data(), key.size()), log_message);
    if (!status.ok())
    {
        LOG(ERROR) << "add log item failed: " << status.ToString()
                   << ", RocksDB error code: " << (int) status.code()
                   << ", tx_number: " << tx_number
                   << ", timestamp: " << timestamp;
    }

    return (int) status.code();
}

std::pair<bool, std::unique_ptr<ItemIterator>>
LogStateRocksDBImpl::GetLogReplayList(uint64_t start_timestamp)
{
    std::vector<Item::Pointer> ddl_list;

    GetSchemaOpList(ddl_list);
    size_t schema_size = ddl_list.size();
    LOG(INFO) << "schema_log_list size: " << schema_size;

    GetSplitRangeOpList(ddl_list);
    size_t rs_size = ddl_list.size() - schema_size;
    LOG(INFO) << "split_range_op_list size: " << rs_size;

    std::unique_ptr<ItemIterator> result =
        std::make_unique<ItemIteratorRocksDBImpl>(
            rocksdb_scan_threads_, std::move(ddl_list), db_, start_timestamp);

    return std::make_pair(true, std::move(result));
}

std::pair<bool, Item::Pointer> LogStateRocksDBImpl::SearchTxDataLog(
    uint64_t tx_number, uint64_t lower_bound_ts)
{
    LOG(INFO) << "log state search tx: " << tx_number
              << ", lower_bound_ts: " << lower_bound_ts;

    // search tx log from last_ckpt_ts + 1
    uint64_t start_ts = 0;
    if (lower_bound_ts != 0)
    {
        start_ts = lower_bound_ts - 1;
    }
    else
    {
        start_ts = LastCkptTimestamp();
        if (start_ts != 0)  // 0 indicates no checkpoint happened
        {
            start_ts += 1;
        }
    }
    LOG(INFO) << "iterate log records, start ts: " << start_ts;

    // iterate log records in range [start, limit)
    std::array<char, 16> start_key{};
    Serialize(start_key, start_ts, 0);
    rocksdb::Slice start(start_key.data(), start_key.size());
    rocksdb::ReadOptions read_option;
    // set iterate_upper_bound for read_option for better performance
    // use with prefix_extractor?
    read_option.iterate_lower_bound = &start;
    auto it = db_->NewIterator(read_option);
    std::array<char, 8> target_txn{};
    Item::Pointer ptr = nullptr;
    for (int idx = 0, shift = 56; shift >= 0; idx++, shift -= 8)
    {
        target_txn[idx] = (tx_number >> shift) & 0xff;
    }
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::string_view key_sv = it->key().ToStringView();
        // key_sv is in form of timestamp(8) + tx_no(8)
        if (key_sv.compare(8, 8, target_txn.data(), 8) == 0)
        {
            uint64_t ts;
            uint64_t tx_no;
            Deserialize(it->key(), ts, tx_no);
            LOG(INFO) << "Found matching key, tx_no: " << tx_no
                      << ", timestamp: " << ts;
            ptr = std::make_shared<Item>(
                tx_number, ts, it->value().ToString(), LogItemType::DataLog);
            break;
        }
    }
    if (!it->status().ok())
    {
        LOG(ERROR) << "Iterate failed: " << it->status().ToString();
    }
    assert(it->status().ok());
    delete it;
    if (ptr == nullptr)
    {
        LOG(INFO) << "tx: " << tx_number << " not found in log state";
    }
    return std::make_pair(true, ptr);
}

/**
 * Start will be called in starting LogState.
 */
int LogStateRocksDBImpl::Start()
{
    // before opening rocksdb, rocksdb_storage_path_ must exist, create it if
    // not exist
    std::error_code error_code;
    bool rocksdb_storage_path_exists =
        std::filesystem::exists(rocksdb_storage_path_, error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << "unable to check rocksdb directory: "
                   << rocksdb_storage_path_
                   << ", error code: " << error_code.value()
                   << ", error message: " << error_code.message();
        return -1;
    }
    if (!rocksdb_storage_path_exists)
    {
        std::filesystem::create_directories(rocksdb_storage_path_, error_code);
        if (error_code.value() != 0)
        {
            LOG(ERROR) << "unable to create rocksdb directory: "
                       << rocksdb_storage_path_
                       << ", error code: " << error_code.value()
                       << ", error message: " << error_code.message();
            return -1;
        }
    }

    // open local rocksdb if its not rocksdb cloud
    LOG(INFO) << "Starting log state local rocksdb";
    if (db_ != nullptr)
    {
        LOG(INFO) << "log state Rocksdb already started";
        return 0;
    }

    rocksdb::Status status;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    // This option is important, this set disable_auto_compaction to
    // false will half the throughput (100MB/s -> 50MB/s)
    //
    // On low end machine, we observed write log latency vibration when
    // compaction happen, so we changed log key to be prefixed by timestamp
    // instead of ng_id, which in turn avoiding the necessarity of compaction
    options.disable_auto_compactions = true;

    // we don't need compaction style since we disabled compaction
    // Set compation style to universal can improve throughput by 2x
    // options.compaction_style = rocksdb::kCompactionStyleUniversal;

    // Since no compaction is necessary, we can have only 1 level of sst file
    options.num_levels = 1;
    options.info_log_level = rocksdb::INFO_LEVEL;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;

    // Enable statistics to get memtabls size
    options.statistics = rocksdb::CreateDBStatistics();

    // set listener for flush events
    auto db_event_listener = std::make_shared<RocksDBEventListener>(this);
    options.listeners.emplace_back(db_event_listener);

    std::vector<rocksdb::ColumnFamilyDescriptor> cfds;
    cfds.emplace_back(rocksdb::kDefaultColumnFamilyName, options);
    cfds.emplace_back(meta_family, options);

    std::vector<std::string> column_families;
    status = rocksdb::DB::ListColumnFamilies(
        options, rocksdb_storage_path_, &column_families);

    if (!status.ok())
    {
        LOG(WARNING) << "Failed to list column families of the RocksDB "
                        "log: "
                     << status.ToString();
    }

    bool new_db = false;
    if (column_families.size() <= 1)
    {
        LOG(INFO) << "No existing column families found. Creating "
                     "default and meta column families.";
        std::vector<rocksdb::ColumnFamilyHandle *> cfhs;
        status = rocksdb::DB::Open(
            options, rocksdb_storage_path_, cfds, &cfhs, &db_);
        if (!status.ok())
        {
            LOG(ERROR) << "Failed to open the RocksDB log, error: "
                       << status.ToString();
            return -1;
        }

        default_handle_ = cfhs[0];
        meta_handle_ = cfhs[1];
        new_db = true;
    }
    else if (column_families.size() != 2)
    {
        LOG(ERROR) << "The RocksDB log is corrupted with incorrect number "
                      "of column families.";
        return -1;
    }
    else
    {
        std::vector<rocksdb::ColumnFamilyHandle *> cfhs;
        status = rocksdb::DB::Open(
            options, rocksdb_storage_path_, cfds, &cfhs, &db_);

        if (!status.ok())
        {
            LOG(ERROR) << "Failed to open the RocksDB log, error: "
                       << status.ToString();
            return -1;
        }

        default_handle_ = cfhs[0];
        meta_handle_ = cfhs[1];
    }

    // Recover meta data from RocksDB to log_state
    if (!new_db)
    {
        rocksdb::ReadOptions read_options;

        // last_ckpt_ts
        std::array<char, 17> key;
        std::string value;
        Serialize(key,
                  UINT64_MAX,
                  UINT64_MAX,
                  static_cast<uint8_t>(LogState::MetaOp::LastCkpt));
        rocksdb::Status rc = db_->Get(read_options,
                                      meta_handle_,
                                      rocksdb::Slice(key.data(), key.size()),
                                      &value);
        if (rc.ok())
        {
            cc_ng_info_.last_ckpt_ts_ =
                *reinterpret_cast<uint64_t *>(value.data());
        }
        else if (rc.IsNotFound())
        {
            cc_ng_info_.last_ckpt_ts_ = 0;
        }
        else
        {
            LOG(ERROR) << "Failed to get last checkpoint timestamp from "
                          "rocksdb, rocksdb storage path: "
                       << rocksdb_storage_path_
                       << ", error message: " << rc.ToString();
            assert(false);
            return -1;
        }

        // latest_txn_no
        Serialize(
            key, UINT64_MAX, UINT64_MAX, (uint8_t) LogState::MetaOp::MaxTxn);
        rc = db_->Get(read_options,
                      meta_handle_,
                      rocksdb::Slice(key.data(), key.size()),
                      &value);

        if (rc.ok())
        {
            cc_ng_info_.latest_txn_no_ = *((uint32_t *) value.data());
        }
        else if (rc.IsNotFound())
        {
            cc_ng_info_.latest_txn_no_ = 0;
        }
        else
        {
            LOG(ERROR)
                << "Failed to get last txn from rocksdb, rocksdb storage path: "
                << rocksdb_storage_path_
                << ", error message: " << rc.ToString();
            assert(false);
            return -1;
        }

        // tx_catalog_ops and tx_split_range_ops_
        {
            rocksdb::Iterator *it;
            it = db_->NewIterator(read_options, meta_handle_);
            if (it == nullptr)
            {
                std::cerr << "Failed to create iterator for meta column family."
                          << std::endl;
                return -1;
            }

            // Get the entry with the lowest key(smallest tx_number+stage)
            it->SeekToFirst();

            uint64_t timestamp = 0;
            uint64_t tx_number = 0;
            SplitRangeOpMessage_Stage new_range_stage;
            SplitRangeOpMessage new_range_op_msg;

            while (it->Valid())
            {
                ::google::protobuf::RepeatedPtrField<SchemaOpMessage>
                    new_schemas_op_msg;
                // Get the key and value as rocksdb::Slice
                rocksdb::Slice key_slice = it->key();
                rocksdb::Slice value_slice = it->value();

                // Convert rocksdb::Slice to std::string for easier handling
                std::string key_str = key_slice.ToString();
                std::string value_str = value_slice.ToString();

                // Check if value_str is not empty before accessing the last
                // character
                assert(!value_str.empty());
                // Get the last character of key_str
                char last_char = key_str.back();

                // Convert LogState::MetaOp::SchemaOp to uint8_t for
                // comparison
                uint8_t schema_op_uint8 =
                    static_cast<uint8_t>(LogState::MetaOp::SchemaOp);
                uint8_t range_op_uint8 =
                    static_cast<uint8_t>(LogState::MetaOp::RangeOp);

                // Compare the last character with the uint8_t
                // representation of the enum
                if (static_cast<uint8_t>(last_char) == schema_op_uint8)
                {
                    Deserialize(key_slice,
                                value_slice,
                                timestamp,
                                tx_number,
                                new_schemas_op_msg);

                    auto [it, success] = tx_catalog_ops_.try_emplace(
                        tx_number, new_schemas_op_msg, timestamp);

                    if (!success)
                    {
                        auto catalog_it = tx_catalog_ops_.find(tx_number);
                        assert(catalog_it != tx_catalog_ops_.end());
                        // The schema operation has been logged. Only
                        // updates the stage.
                        assert(static_cast<int>(
                                   catalog_it->second.SchemaOpMsgCount()) ==
                               new_schemas_op_msg.size());

                        for (uint16_t idx = 0;
                             idx < catalog_it->second.SchemaOpMsgCount();
                             ++idx)
                        {
                            SchemaOpMessage &msg =
                                catalog_it->second.SchemaOpMsgs()[idx];
                            SchemaOpMessage &new_msg =
                                new_schemas_op_msg.at(idx);
                            if (new_schemas_op_msg.at(idx).stage() >
                                msg.stage())
                            {
                                msg.set_stage(new_msg.stage());
                            }
                        }
                    }
                }
                else if (static_cast<uint8_t>(last_char) == range_op_uint8)
                {
                    Deserialize(key_slice,
                                value_slice,
                                timestamp,
                                tx_number,
                                new_range_op_msg);
                    new_range_stage = new_range_op_msg.stage();

                    auto [it_range, success_range] =
                        tx_split_range_ops_.try_emplace(
                            tx_number, new_range_op_msg, timestamp);

                    if (!success_range)
                    {
                        auto range_it = tx_split_range_ops_.find(tx_number);
                        assert(range_it != tx_split_range_ops_.end());

                        // The schema operation has been logged. Only
                        // updates the stage.
                        SplitRangeOpMessage &existing_range_op_msg =
                            range_it->second.split_range_op_message_;

                        if (new_range_stage > existing_range_op_msg.stage())
                        {
                            existing_range_op_msg.set_stage(new_range_stage);
                        }
                    }
                }

                uint32_t latest_txn_no =
                    cc_ng_info_.latest_txn_no_.load(std::memory_order_relaxed);
                // assuming the range of transaction number within UINT32_MAX/2
                if (static_cast<int32_t>(
                        static_cast<uint32_t>(tx_number & 0xFFFFFFFF) -
                        latest_txn_no) > 0)
                {
                    cc_ng_info_.latest_txn_no_.store(latest_txn_no,
                                                     std::memory_order_relaxed);
                }
                // Move to the next key
                it->Next();
            }

            // Check for iterator status after iteration
            if (!it->status().ok())
            {
                std::cerr << "Error during iteration: "
                          << it->status().ToString() << std::endl;
            }
            delete it;
        }
    }

    write_option_.disableWAL = false;
    write_option_.sync = true;
    LOG(INFO) << "The RocksDb log started.";

    stop_purge_thread_.store(false, std::memory_order_release);
    purge_thread_ = std::thread(&LogStateRocksDBImpl::PurgingSstFiles, this);

    return 0;
}

void LogStateRocksDBImpl::StopRocksDB()
{
    {
        std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
        stop_purge_thread_.store(true, std::memory_order_release);
    }
    sst_queue_cv_.notify_all();

    if (purge_thread_.joinable())
    {
        purge_thread_.join();
    }

    if (db_ != nullptr)
    {
        LOG(INFO) << "DestroyColumnFamilyHandle.";
        db_->DestroyColumnFamilyHandle(default_handle_);
        db_->DestroyColumnFamilyHandle(meta_handle_);

        LOG(INFO) << "Closing RocksDB database.";
        db_->Close();
        delete db_;
        db_ = nullptr;
        LOG(INFO) << "RocksDB database object deleted, db_ set to nullptr.";
    }
}

/**
 * for debug use only
 */
void LogStateRocksDBImpl::PrintKey(rocksdb::Slice key)
{
    uint64_t timestamp, tx_no;
    Deserialize(key, timestamp, tx_no);
    LOG(INFO) << "timestamp: " << timestamp << ",\ttx_number: " << tx_no;
}

/**
 * Close db_ and remove all files and dirs in rocksdb storage path but keep
 * the directory itself.
 * Create rocksdb_storage_path directory if not exists (e.g. when an instance
 * first starts, starting braft::node will load snapshot but rocksdb hasn't
 * started yet) for later copying snapshot.
 */
void LogStateRocksDBImpl::CloseAndClearRocksDB()
{
    {
        std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
        stop_purge_thread_.store(true, std::memory_order_release);
    }
    sst_queue_cv_.notify_all();

    if (purge_thread_.joinable())
    {
        purge_thread_.join();
    }

    // first close db_
    if (db_ != nullptr)
    {
        auto status = db_->Close();
        if (!status.ok())
        {
            LOG(ERROR) << "close rocksdb failed: " << status.ToString();
        }
        assert(status.ok());
        delete db_;
        db_ = nullptr;
        LOG(INFO) << "rocksdb instance closed";
    }

    // clear all files in rocksdb_storage_path_
    std::uintmax_t files_deleted = 0;
    std::error_code error_code;
    std::filesystem::directory_iterator dir_ite(rocksdb_storage_path_,
                                                error_code);
    if (error_code.value() != 0)
    {
        LOG(ERROR) << error_code.value() << " " << error_code.message();
    }
    else
    {
        for (const auto &entry : dir_ite)
        {
            files_deleted +=
                std::filesystem::remove_all(entry.path(), error_code);
            if (error_code.value() != 0)
            {
                LOG(ERROR) << error_code.value() << " " << error_code.message();
            }
        }
    }
    LOG(INFO) << "clear rocksdb storage path, " << files_deleted
              << " files or directories removed";
}

void LogStateRocksDBImpl::NotifySstFileCreated(
    const rocksdb::FlushJobInfo &flush_job_info)
{
    std::lock_guard<bthread::Mutex> lk(sst_queue_mutex_);
    sst_created_queue_.push(flush_job_info);
    sst_queue_cv_.notify_all();
}

uint64_t LogStateRocksDBImpl::GetSstFilesSize()
{
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    uint64_t size = 0;
    for (const auto &meta : metadata)
    {
        size += meta.size;
    }
    return size;
}

void LogStateRocksDBImpl::PurgingSstFiles()
{
    // calculate sst files size on disk at start
    sst_files_size_ = GetSstFilesSize();
    double files_size_at_sart =
        static_cast<double>(sst_files_size_) / 1024 / 1024;
    LOG(INFO) << "Sst files size on disk: " << std::fixed
              << std::setprecision(1) << files_size_at_sart << "MB";

    std::unique_lock<bthread::Mutex> lk(sst_queue_mutex_);
    while (!stop_purge_thread_.load(std::memory_order_acquire))
    {
        if (!lk.owns_lock())
        {
            lk.lock();
        }
        sst_queue_cv_.wait(lk);
        // accumulate sst files size
        while (!sst_created_queue_.empty())
        {
            // purge sst file
            auto flush_job_info = sst_created_queue_.front();
            sst_created_queue_.pop();
            lk.unlock();
            sst_files_size_ += flush_job_info.table_properties.data_size +
                               flush_job_info.table_properties.index_size +
                               flush_job_info.table_properties.filter_size;
            DLOG(INFO) << "sst files size on disk: " << std::fixed
                       << std::setprecision(1)
                       << static_cast<double>(sst_files_size_) / 1024 / 1024
                       << "MB";
            if (sst_files_size_ > sst_files_size_limit_)
            {
                break;
            }
            lk.lock();
        }

        // purge sst files if ssd files size exceeds limit
        if (sst_files_size_ > sst_files_size_limit_ &&
            !stop_purge_thread_.load(std::memory_order_acquire))
        {
            if (lk.owns_lock())
            {
                lk.unlock();
            }
            LOG(INFO) << "Sst files size on disk exceeds limit: " << std::fixed
                      << std::setprecision(1)
                      << static_cast<double>(sst_files_size_) / 1024 / 1024
                      << "MB, purge sst files";

            const auto &ng_info = GetCcNgInfo();
            // find last_ckpt_ts
            uint64_t min_last_ckpt_ts = ng_info.last_ckpt_ts_;
            if (min_last_ckpt_ts == 0)
            {
                LOG(INFO) << "No checkpoint found, skip purge sst files";
                continue;
            }

            if (min_last_ckpt_ts <= last_purging_sst_ckpt_ts_)
            {
                // skip purging sst file if ckpt_ts is not updated
                LOG(INFO) << "The mininum checkpoint not been updated, skip "
                             "purge sst files, last_purging_sst_ckpt_ts_: "
                          << last_purging_sst_ckpt_ts_;
                continue;
            }
            DLOG(INFO) << "last_purging_sst_chpt_ts_: "
                       << last_purging_sst_ckpt_ts_
                       << " ,new min_last_chpt_ts: " << min_last_ckpt_ts;
            last_purging_sst_ckpt_ts_ = min_last_ckpt_ts;

            // prepare the range to delete
            // all log entries before the min_last_ckpt_ts belongs to all cc
            // node group could be deleted
            min_last_ckpt_ts -= 1;
            std::array<char, 16> start_key{};
            std::array<char, 16> end_key{};
            Serialize(start_key, 0, 0);
            Serialize(end_key, min_last_ckpt_ts, 0);
            rocksdb::Slice start(start_key.data(), start_key.size());
            rocksdb::Slice end(end_key.data(), end_key.size());

            std::vector<std::string> delete_files;
            std::vector<rocksdb::LiveFileMetaData> metadata;
            db_->GetLiveFilesMetaData(&metadata);
            for (const auto &meta : metadata)
            {
                if (meta.column_family_name == default_handle_->GetName())
                {
                    rocksdb::Slice smallestkey(meta.smallestkey);
                    rocksdb::Slice largestkey(meta.largestkey);
#ifndef NDEBUG
                    uint64_t sk_ts, sk_tx_no;
                    assert(smallestkey.size() == 16);
                    Deserialize(smallestkey, sk_ts, sk_tx_no);
                    uint64_t lk_ts, lk_tx_no;
                    assert(largestkey.size() == 16);
                    Deserialize(largestkey, lk_ts, lk_tx_no);
                    DLOG(INFO)
                        << "sst file: " << meta.name << ", size: " << meta.size
                        << ", level: " << meta.level
                        << " smallest key: " << sk_ts << ", " << sk_tx_no
                        << " largest key: " << lk_ts << ", " << lk_tx_no;
#endif

                    // without compaction, sst files must in level 0
                    if (meta.level == 0 && meta.size > 0 &&
                        largestkey.compare(end) < 0 &&
                        smallestkey.compare(start) > 0)
                    {
                        delete_files.emplace_back(meta.name);
                    }
                }
            }

            std::sort(delete_files.begin(), delete_files.end());

            // delete sst files
            for (auto &file : delete_files)
            {
                DLOG(INFO) << "SST file: " << file << " purged.";
                auto status = db_->DeleteFile(file);
                if (!status.ok())
                {
                    LOG(ERROR)
                        << "Purge sst file failed: " << status.ToString();
                }
            }

            double files_size_before =
                static_cast<double>(sst_files_size_) / 1024 / 1024;
            sst_files_size_ = GetSstFilesSize();
            double files_size_after =
                static_cast<double>(sst_files_size_) / 1024 / 1024;
            LOG(INFO) << "Purge sst files, size before: " << std::fixed
                      << std::setprecision(1) << files_size_before
                      << "MB, size after: " << files_size_after << "MB, "
                      << (files_size_before - files_size_after) << "MB purged"
                      << " ,ckpt_ts: " << min_last_ckpt_ts;
            metadata.clear();
#ifndef NDEBUG
            // print live files after purge
            db_->GetLiveFilesMetaData(&metadata);
            for (const auto &meta : metadata)
            {
                if (meta.column_family_name == default_handle_->GetName())
                {
                    rocksdb::Slice smallestkey(meta.smallestkey);
                    rocksdb::Slice largestkey(meta.largestkey);
                    uint64_t sk_ts, sk_tx_no;
                    assert(smallestkey.size() == 16);
                    Deserialize(smallestkey, sk_ts, sk_tx_no);
                    uint64_t lk_ts, lk_tx_no;
                    assert(largestkey.size() == 16);
                    Deserialize(largestkey, lk_ts, lk_tx_no);
                    DLOG(INFO)
                        << "sst file: " << meta.name << ", size: " << meta.size
                        << ", level: " << meta.level
                        << " smallest key: " << sk_ts << ", " << sk_tx_no
                        << " largest key: " << lk_ts << ", " << lk_tx_no;
                }
            }
#endif
        }
    }
}

int LogStateRocksDBImpl::PersistSchemaOp(uint64_t txn,
                                         uint64_t timestamp,
                                         const std::string &schema_op_str)
{
    std::array<char, 17> key{};
    rocksdb::ReadOptions read_options;

    Serialize(key, timestamp, txn, static_cast<uint8_t>(MetaOp::SchemaOp));
    const rocksdb::Status rc = db_->Put(write_option_,
                                        meta_handle_,
                                        rocksdb::Slice(key.data(), key.size()),
                                        schema_op_str);
    return rc.ok() ? 0 : rc.code();
}

int LogStateRocksDBImpl::PersistSchemasOp(
    uint64_t txn,
    uint64_t timestamp,
    const ::google::protobuf::RepeatedPtrField<SchemaOpMessage> &schemas_op)
{
    std::array<char, 17> key{};
    Serialize(key, timestamp, txn, static_cast<uint8_t>(MetaOp::SchemaOp));

    std::string schemas_op_str;

    uint16_t cnt = schemas_op.size();
    schemas_op_str.append(reinterpret_cast<char *>(&cnt), sizeof(cnt));

    for (const SchemaOpMessage &msg : schemas_op)
    {
        std::string str = msg.SerializeAsString();
        uint32_t len = str.size();
        schemas_op_str.append(reinterpret_cast<char *>(&len), sizeof(len));
        schemas_op_str += str;
    }

    rocksdb::Status rc =
        db_->Put(write_option_,
                 meta_handle_,
                 rocksdb::Slice(key.data(), key.size()),
                 rocksdb::Slice(schemas_op_str.data(), schemas_op_str.size()));
    return rc.ok() ? 0 : rc.code();
}

int LogStateRocksDBImpl::DeleteSchemaOp(uint64_t txn, uint64_t timestamp)
{
    std::array<char, 17> key{};
    Serialize(key, timestamp, txn, static_cast<uint8_t>(MetaOp::SchemaOp));

    rocksdb::Status rc = db_->Delete(
        write_option_, meta_handle_, rocksdb::Slice(key.data(), key.size()));

    return rc.ok() ? 0 : rc.code();
}

int LogStateRocksDBImpl::PersistRangeOp(uint64_t txn,
                                        uint64_t timestamp,
                                        const std::string &range_op_str)
{
    std::array<char, 17> key{};
    Serialize(key, timestamp, txn, static_cast<uint8_t>(MetaOp::RangeOp));

    const rocksdb::Status rc = db_->Put(write_option_,
                                        meta_handle_,
                                        rocksdb::Slice(key.data(), key.size()),
                                        range_op_str);

    return rc.ok() ? 0 : rc.code();
}

int LogStateRocksDBImpl::DeleteRangeOp(uint64_t txn, uint64_t timestamp)
{
    std::array<char, 17> key{};
    Serialize(key, timestamp, txn, (uint8_t) LogState::MetaOp::RangeOp);

    rocksdb::Status rc = db_->Delete(
        write_option_, meta_handle_, rocksdb::Slice(key.data(), key.size()));

    return rc.ok() ? 0 : rc.code();
}

int LogStateRocksDBImpl::PersistCkptAndMaxTxn(uint64_t ckpt_ts,
                                              uint32_t max_txn)
{
    const char *ckpt_ptr = (const char *) (&ckpt_ts);
    const char *txn_ptr = (const char *) (&max_txn);

    std::array<char, 17> key;

    Serialize(
        key, UINT64_MAX, UINT64_MAX, (uint8_t) LogState::MetaOp::LastCkpt);
    rocksdb::Status rc = db_->Put(write_option_,
                                  meta_handle_,
                                  rocksdb::Slice(key.data(), key.size()),
                                  rocksdb::Slice(ckpt_ptr, sizeof(uint64_t)));
    if (!rc.ok())
    {
        LOG(ERROR) << "PersistCkptAndMaxTxn (LastCkpt) failed, error: "
                   << rc.ToString() << ", ckpt_ts: " << ckpt_ts;
        return rc.code();
    }

    Serialize(key, UINT64_MAX, UINT64_MAX, (uint8_t) LogState::MetaOp::MaxTxn);
    rc = db_->Put(write_option_,
                  meta_handle_,
                  rocksdb::Slice(key.data(), key.size()),
                  rocksdb::Slice(txn_ptr, sizeof(uint32_t)));

    return rc.ok() ? 0 : rc.code();
}

uint64_t LogStateRocksDBImpl::GetApproximateReplayLogSize()
{
    uint64_t total_memtable_size = 0;
    uint64_t total_sst_size = 0;

    /* Get total memtables size */
    std::string size_val;
    if (db_->GetProperty("rocksdb.cur-size-all-mem-tables", &size_val))
    {
        total_memtable_size = std::stoi(size_val);
    }
    else
    {
        LOG(ERROR) << "Failed to get memory table size";
    }

    /* Get total SStables size used in Tx recovery  */
    std::vector<rocksdb::LiveFileMetaData> metadata;
    db_->GetLiveFilesMetaData(&metadata);
    for (const auto &meta : metadata)
    {
        if (meta.column_family_name == default_handle_->GetName())
        {
            rocksdb::Slice largestkey(meta.largestkey);
            uint64_t lk_ts, lk_tx_no;
            assert(largestkey.size() == 16);
            Deserialize(largestkey, lk_ts, lk_tx_no);

            if (cc_ng_info_.last_ckpt_ts_ <= lk_ts)
            {
                DLOG(INFO) << "SSTable " << meta.name
                           << " used in replay with size: "
                           << FormatSize(meta.size);
                total_sst_size += meta.size;
            }
        }
    }

    LOG(INFO) << "Total memtables size: " << FormatSize(total_memtable_size);
    LOG(INFO) << "Total SSTables size used in replay: "
              << FormatSize(total_sst_size);

    uint64_t total_txlog_size = total_memtable_size + total_sst_size;
    LOG(INFO) << "Total replay log size: " << FormatSize(total_txlog_size);
    return total_txlog_size;
}

void RocksDBEventListener::OnFlushCompleted(
    rocksdb::DB *db, const rocksdb::FlushJobInfo &flush_job_info)
{
    size_t file_size = flush_job_info.table_properties.data_size +
                       flush_job_info.table_properties.index_size +
                       flush_job_info.table_properties.filter_size;
    LOG(INFO) << "Flush sst end, file: " << flush_job_info.file_path
              << " ,job_id: " << flush_job_info.job_id
              << " ,thread: " << flush_job_info.thread_id
              << " ,file_number: " << flush_job_info.file_number
              << " ,triggered_writes_slowdown: "
              << flush_job_info.triggered_writes_slowdown
              << " ,triggered_writes_stop: "
              << flush_job_info.triggered_writes_stop
              << " ,smallest_seqno: " << flush_job_info.smallest_seqno
              << " ,largest_seqno: " << flush_job_info.largest_seqno
              << " ,flush_reason: "
              << GetFlushReason(flush_job_info.flush_reason) << std::fixed
              << std::setprecision(1)
              << " ,file_size: " << static_cast<double>(file_size) / 1024 / 1024
              << "MB" << std::endl;

    log_state_->NotifySstFileCreated(flush_job_info);
}

std::string RocksDBEventListener::GetFlushReason(
    rocksdb::FlushReason flush_reason)
{
    switch (flush_reason)
    {
    case rocksdb::FlushReason::kOthers:
        return "kOthers";
    case rocksdb::FlushReason::kGetLiveFiles:
        return "kGetLiveFiles";
    case rocksdb::FlushReason::kShutDown:
        return "kShutDown";
    case rocksdb::FlushReason::kExternalFileIngestion:
        return "kExternalFileIngestion";
    case rocksdb::FlushReason::kManualCompaction:
        return "kManualCompaction";
    case rocksdb::FlushReason::kWriteBufferManager:
        return "kWriteBufferManager";
    case rocksdb::FlushReason::kWriteBufferFull:
        return "kWriteBufferFull";
    case rocksdb::FlushReason::kTest:
        return "kTest";
    case rocksdb::FlushReason::kDeleteFiles:
        return "kDeleteFiles";
    case rocksdb::FlushReason::kAutoCompaction:
        return "kAutoCompaction";
    case rocksdb::FlushReason::kManualFlush:
        return "kManualFlush";
    case rocksdb::FlushReason::kErrorRecovery:
        return "kErrorRecovery";
    case rocksdb::FlushReason::kErrorRecoveryRetryFlush:
        return "kErrorRecoveryRetryFlush";
    case rocksdb::FlushReason::kWalFull:
        return "kWalFull";
    default:
        return "unknown";
    }
}
}  // namespace txlog
