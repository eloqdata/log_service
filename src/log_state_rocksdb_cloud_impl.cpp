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
#if defined(USE_ROCKSDB_LOG_STATE) && defined(WITH_ROCKSDB_CLOUD)

#include <bthread/bthread.h>

#include <filesystem>
#include <future>
#include <memory>
#include <system_error>

#include "fault_inject.h"
#include "log_state_rocksdb_impl.h"
#include "log_utils.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/convenience.h"
#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/sst_file_reader.h"
#include "rocksdb/statistics.h"
#include "rocksdb_cloud_config.h"

namespace ROCKSDB_NAMESPACE
{
extern std::string MakeTableFileName(uint64_t number);
}

namespace txlog
{

CronJob::CronJob(const std::string &job_name,
                 current_time_func current_time,
                 std::chrono::seconds check_interval)
    : job_name_(job_name),
      wait_mutex_(),
      wait_cv_(),
      is_canceled_(false),
      is_started_(false),
      current_time_(current_time),
      check_interval_(check_interval)
{
}

void CronJob::Start(uint32_t &days_from_now,
                    uint32_t &starting_hour,
                    uint32_t &starting_minute,
                    uint32_t &starting_second,
                    std::function<void(current_time_func)> job,
                    bool repeatable)
{
    if (is_started_.load(std::memory_order_acquire))
    {
        return;
    }

    is_started_.store(true, std::memory_order_release);

    thd_ = std::thread(
        [this,
         days_from_now,
         starting_hour,
         starting_minute,
         starting_second,
         job,
         repeatable]()
        {
            std::time_t current = current_time_(nullptr);
            std::time_t run_time = CalculateNextRunTime(
                days_from_now, starting_hour, starting_minute, starting_second);
            if (current >= run_time)
            {
                LOG(ERROR) << "The scheduled cron job time is early than "
                              "current time.";
                return;
            }

            while (!is_canceled_.load(std::memory_order_acquire))
            {
                // Get the current system time
                current = current_time_(nullptr);

                if (current >= run_time)
                {
                    job(current_time_);
                    // if the cron job is not repeatable
                    if (!repeatable)
                        break;
                    // if the cron job is repeatable, and the days from now
                    // is zero, then schedule the job on the next day
                    run_time = CalculateNextRunTime(
                        days_from_now == 0 ? 1 : days_from_now,
                        starting_hour,
                        starting_minute,
                        starting_second);
                }

                std::unique_lock<std::mutex> lk(wait_mutex_);
                bool is_canceled = wait_cv_.wait_for(
                    lk,
                    check_interval_,
                    [&]
                    { return is_canceled_.load(std::memory_order_acquire); });

                if (is_canceled)
                {
                    LOG(INFO) << "CronJob " << job_name_ << " canceled.";
                    break;
                }
                // otherwise, cv.wait_for time out, now doing the time check
            }
        });
}

void CronJob::Cancel()
{
    if (is_canceled_.load(std::memory_order_acquire))
    {
        return;
    }
    is_canceled_.store(true, std::memory_order_release);

    {
        std::unique_lock<std::mutex> lk(wait_mutex_);
        wait_cv_.notify_one();
    }

    if (thd_.joinable())
    {
        thd_.join();
    }
}
std::time_t CronJob::CalculateNextRunTime(uint16_t days_from_now,
                                          uint16_t starting_hour,
                                          uint16_t starting_minute,
                                          uint16_t starting_second)
{
    // Get the current system time
    std::time_t current = current_time_(nullptr);

    // Convert the current time to a local time struct
    std::tm *local_time = std::localtime(&current);

    // Increment the day by 1
    local_time->tm_mday = local_time->tm_mday + days_from_now;

    // Set the desired time (default is 00:00:01)
    local_time->tm_hour = starting_hour;
    local_time->tm_min = starting_minute;
    local_time->tm_sec = starting_second;

    return std::mktime(local_time);
}

void DBCloudContainer::Open(const rocksdb::CloudFileSystemOptions &cfs_options,
                            const std::string &rocksdb_storage_path,
                            const int max_write_buffer_number,
                            const int max_background_jobs,
                            const uint64_t target_file_size_base)
{
    DLOG(INFO) << "DBCloudContainer Open";
    rocksdb::CloudFileSystem *cfs;
    auto status = NewCloudFileSystem(cfs_options, &cfs);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to create cloud storage filesystem, cloud type: "
#if (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)
                   << "Aws"
#elif (WITH_ROCKSDB_CLOUD == CS_TYPE_GCS)
                   << "Gcp"
#endif
                   << ", at path rocksdb_cloud with bucket "
                   << cfs_options.src_bucket.GetBucketName()
                   << ", with error: " << status.ToString();

        std::abort();
    }

    cloud_fs_.reset(cfs);
    // Create options and use the AWS file system that we created
    // earlier
    cloud_env_ = rocksdb::NewCompositeEnv(cloud_fs_);
    rocksdb::Options options;
    options.env = cloud_env_.get();
    options.create_if_missing = true;

    // This option is important, this set disable_auto_compaction to
    // false will half the throughput (100MB/s -> 50MB/s)
    //
    // On low end machine, we observed write log latency vibration when
    // compaction happen, so we changed log key to be prefixed by timestamp
    // instead of ng_id, which in turn avoiding the necessarity of compaction
    // TODO(XiaoJi): setup remote compaction
    options.disable_auto_compactions = true;
    // keep the default sst file size, 64MB, larger sst file size can not
    // improve perf options.write_buffer_size = 64 * 1024 * 1024;
    options.max_background_jobs = max_background_jobs;
    options.max_write_buffer_number = max_write_buffer_number;
    options.level0_slowdown_writes_trigger = std::numeric_limits<int>::max();
    options.level0_stop_writes_trigger = std::numeric_limits<int>::max();
    // config base sst file size
    options.target_file_size_base = target_file_size_base;

    // we don't need compaction style since we disabled compaction
    // Set compation style to universal can improve throughput by 2x
    // options.compaction_style = rocksdb::kCompactionStyleUniversal;

    // Since we disabled compaction, we only have one level util we setup remote
    // compaction
    options.num_levels = 1;

    options.info_log_level = rocksdb::INFO_LEVEL;
    options.best_efforts_recovery = false;
    options.skip_checking_sst_file_sizes_on_db_open = true;
    options.skip_stats_update_on_db_open = true;
    // Important! keep atomic_flush true, since we disabled WAL
    options.atomic_flush = true;

    // Enable statistics to get memtabls size
    options.statistics = rocksdb::CreateDBStatistics();

    auto db_event_listener = std::make_shared<RocksDBEventListener>();
    options.listeners.emplace_back(db_event_listener);
    // The max_open_files default value is -1, it cause DB open all files on
    // DB::Open() This behavior causes 2 effects,
    // 1. DB::Open() will be slow
    // 2. During DB::Open, some of the opened sst files keep in LRUCache will be
    // deleted due to LRU policy, which causes DB::Open failed
    options.max_open_files = 0;

    status = rocksdb::DBCloud::Open(options, rocksdb_storage_path, "", 0, &db_);

    if (!status.ok())
    {
        LOG(ERROR) << "Unable to open db at path " << rocksdb_storage_path
                   << " with bucket " << cfs_options.src_bucket.GetBucketName()
                   << " with error: " << status.ToString();
        std::abort();
    }

    // Reset max_open_files to default value of -1 after DB::Open
    db_->SetDBOptions({{"max_open_files", "-1"}});

    is_open_.store(true, std::memory_order_release);
    LOG(INFO) << "RocksDB Cloud started";
}

DBCloudContainer::~DBCloudContainer()
{
    DLOG(INFO) << "~DBCloudContainer()";
    is_open_.store(false, std::memory_order_release);
    // prevent below "if" reordered before above "store"
    std::atomic_thread_fence(std::memory_order_seq_cst);

    if (db_ != nullptr)
    {
        db_->Close();
        delete db_;
        db_ = nullptr;

        cloud_env_ = nullptr;
        cloud_fs_ = nullptr;
    }
}

bool LogStateRocksDBCloudImpl::CheckOrWaitForMemDBInSync(
    const std::string &the_waiter, uint32_t timeout_us)
{
    if (mem_db_in_sync_.load(std::memory_order_acquire) == false)
    {
        if (timeout_us == 0)
        {
            timeout_us = cloud_config_.db_ready_timeout_us_;
        }

        LOG(INFO) << the_waiter
                  << " only be called on leader, RocksDB Cloud "
                     "is rolling up, wait for it becomming ready, timeout: "
                  << timeout_us;

        if (!BthreadCondWaitFor(
                mem_db_in_sync_mutex_,
                mem_db_in_sync_cv_,
                timeout_us,
                [this]
                {
                    return term_if_is_lg_leader_.load(
                               std::memory_order_acquire) == -1 ||
                           mem_db_in_sync_.load(std::memory_order_acquire) ==
                               true;
                }))
        {
            // time out
            LOG(ERROR) << the_waiter
                       << " wait for RocksDB Cloud rolling up timeout";
            return false;
        }

        // not lg leader
        if (term_if_is_lg_leader_.load(std::memory_order_acquire) == -1)
        {
            LOG(ERROR) << the_waiter
                       << " wait for RocksDB Cloud rolling up, but lg node is "
                          "no longer leader";
            return false;
        }

        // mem db in sync
        LOG(INFO) << "RocksDB Cloud is ready, " << the_waiter
                  << " resuming proceed!";
    }

    return true;
}

void LogStateRocksDBCloudImpl::PurgeDBCloudFiles(
    const std::shared_ptr<DBCloudContainer> &dbc_purge_log,
    uint32_t log_retention_days)
{
    // Only the leader can see the rocksdb cloud is opened
    if (dbc_purge_log->IsOpened())
    {
        // Get the current time as
        // std::chrono::system_clock::time_point
        std::chrono::system_clock::time_point current =
            std::chrono::system_clock::now();

        // Subtract retention days from the current time
        std::chrono::system_clock::time_point days_ago =
            current - std::chrono::hours(24 * log_retention_days);

        // truncate time to 00:00:00
        std::time_t days_ago_t = std::chrono::system_clock::to_time_t(days_ago);
        std::tm *days_ago_tm = std::localtime(&days_ago_t);

        days_ago_tm->tm_hour = 0;
        days_ago_tm->tm_min = 0;
        days_ago_tm->tm_sec = 0;
        days_ago_t = std::mktime(days_ago_tm);

        days_ago = std::chrono::system_clock::from_time_t(days_ago_t);

        // Convert the time point to microseconds
        std::chrono::microseconds days_ago_micro =
            std::chrono::time_point_cast<std::chrono::microseconds>(days_ago)
                .time_since_epoch();

        // Extract the count of microseconds
        uint64_t purge_t = days_ago_micro.count();

        std::array<char, 20> start_key{};
        std::array<char, 20> end_key{};

        Serialize(start_key, 0, 0, 0);
        Serialize(end_key, purge_t, 0, 0);
        rocksdb::Slice lower_bound =
            rocksdb::Slice(start_key.data(), start_key.size());
        rocksdb::Slice upper_bound =
            rocksdb::Slice(end_key.data(), end_key.size());

        rocksdb::DBCloud *db = dbc_purge_log->GetDBPtr();
        std::vector<std::string> files_to_delete;
        std::vector<rocksdb::LiveFileMetaData> file_meta;
        db->GetLiveFilesMetaData(&file_meta);
        for (auto &meta : file_meta)
        {
            rocksdb::Slice smallestkey(meta.smallestkey);
            rocksdb::Slice largestkey(meta.largestkey);
            uint64_t sk_ts, sk_tx_no;
            uint32_t sk_ng_id;
            Deserialize(smallestkey, sk_ts, sk_ng_id, sk_tx_no);
            uint64_t lk_ts, lk_tx_no;
            uint32_t lk_ng_id;
            Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);
            DLOG(INFO) << "sst file: " << meta.name << ", size: " << meta.size
                       << ", level: " << meta.level
                       << " smallest key: " << sk_ts << ", " << sk_ng_id << ", "
                       << sk_tx_no << " largest key: " << lk_ts << ", "
                       << lk_ng_id << ", " << lk_tx_no;
            // without compaction, sst files must be in level 0
            if (meta.level == 0 && meta.size > 0 &&
                largestkey.compare(upper_bound) < 0 &&
                smallestkey.compare(lower_bound) > 0)
            {
                files_to_delete.emplace_back(meta.name);
            }
        }

        // sort files to delete, because DeleteFile() only accept oldest level 0
        // file
        std::sort(files_to_delete.begin(), files_to_delete.end());

        for (auto &file : files_to_delete)
        {
            LOG(INFO) << "Purging SST file from cloud storage: " << file;
            auto status = db->DeleteFile(file);
            if (!status.ok())
            {
                LOG(ERROR) << "Purge sst file from cloud storage: " << file
                           << " failed: " << status.ToString();
            }
            assert(status.ok());
        }
    }
}

LogStateRocksDBCloudImpl::LogStateRocksDBCloudImpl(
    std::string rocksdb_path,
    const RocksDBCloudConfig &cloud_config,
    const std::atomic<int64_t> &term_if_is_lg_leader,
    LogStateRocksDBCloudImplObserver *observer,
    const size_t in_mem_data_log_queue_size_high_watermark,
    const size_t rocksdb_max_write_buffer_number,
    const size_t rocksdb_max_background_jobs,
    const size_t rocksdb_target_file_size_base,
    const size_t rocksdb_scan_threads)
    : cloud_db_init_thread_(),
      in_mem_state_mutex_(),
      in_mem_data_log_queue_(std::make_unique<std::deque<Item>>()),
      in_mem_data_log_queue_size_high_watermark_(
          in_mem_data_log_queue_size_high_watermark),
      purging_in_mem_data_log_queue_(false),
      log_count_before_purge_(0),
      purge_start_idx_(0),
      rocksdb_max_write_buffer_number_(rocksdb_max_write_buffer_number),
      rocksdb_max_background_jobs_(rocksdb_max_background_jobs),
      rocksdb_target_file_size_base_(rocksdb_target_file_size_base),
      rocksdb_scan_threads_(rocksdb_scan_threads),
      cloud_config_(cloud_config),
      dbc_mutex_(),
      mem_db_in_sync_(false),
      rocksdb_storage_path_(std::move(rocksdb_path)),
      write_option_(),
      snapshot_in_mem_data_log_queue_size_(0),
      snapshot_last_applied_data_tx_(0),
      term_if_is_lg_leader_(term_if_is_lg_leader),
      log_purger_("LogPurger"),
      observer_(observer)
{
    // this dbc_ instance will serve the follower
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }
    write_option_.disableWAL = true;

    // schedule log purge
    uint32_t days_from_now = 0;
    log_purger_.Start(days_from_now,
                      cloud_config_.log_purger_starting_hour_,
                      cloud_config_.log_purger_starting_minute_,
                      cloud_config_.log_purger_starting_second_,
                      [&](current_time_func current_time)
                      {
                          auto log_retention_days =
                              cloud_config_.log_retention_days_;

                          std::shared_ptr<DBCloudContainer> dbc_purge_log;
                          {
                              std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
                              dbc_purge_log = dbc_;
                          }

                          LogStateRocksDBCloudImpl::PurgeDBCloudFiles(
                              dbc_purge_log, log_retention_days);
                      });
};

LogStateRocksDBCloudImpl::~LogStateRocksDBCloudImpl()
{
    WaitForAsyncStartCloudDBFinishIfAny();
    log_purger_.Cancel();
    StopRocksDB();
}

void LogStateRocksDBCloudImpl::AddLogItemToMemState(
    uint32_t cc_ng_id,
    uint64_t tx_number,
    uint64_t timestamp,
    const std::string &log_message)
{
    Item item =
        Item(tx_number, timestamp, log_message, LogItemType::DataLog, cc_ng_id);
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    in_mem_data_log_queue_->emplace_back(std::move(item));
}

void *LogStateRocksDBCloudImpl::AsyncClearInMemoryLogState(void *arg)
{
    auto start = std::chrono::high_resolution_clock::now();
    InMemoryLogStateToClear *sc = static_cast<InMemoryLogStateToClear *>(arg);
    std::unique_ptr<InMemoryLogStateToClear> sc_ptr(sc);
    sc_ptr->in_mem_data_log_queue_->clear();
    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "Delete old in mem state in " << ms.count() << " ms";
    return nullptr;
}

void LogStateRocksDBCloudImpl::PurgeLogItemsFromMemState(
    uint64_t last_applied_tx_number)
{
    LOG(INFO) << "Purging in mem log items with tx number "
              << last_applied_tx_number;

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    size_t affected = 0;
    size_t in_mem_data_log_queue_size = in_mem_data_log_queue_->size();

    // find the pos of last_applied_tx_number, copy the rest to new queue
    size_t iter_idx = in_mem_data_log_queue_size - 1;
    bool found = false;

    // search for the tx_number in mem state, until purge_start_idx_
    auto it = in_mem_data_log_queue_->rbegin();
    for (; it != in_mem_data_log_queue_->rend(); it++)
    {
        if (it->tx_number_ == last_applied_tx_number)
        {
            found = true;
            break;
        }

        if (iter_idx == purge_start_idx_)
        {
            break;
        }

        iter_idx--;
    }

    auto end1 = std::chrono::high_resolution_clock::now();
    auto ms1 =
        std::chrono::duration_cast<std::chrono::milliseconds>(end1 - start);
    LOG(INFO) << "Find last_applied_tx_number in mem state in " << ms1.count()
              << " ms";

    // copy the rest to new queue if found
    if (found)
    {
        // copy the rest to new queue
        auto new_in_mem_data_log_queue = std::make_unique<std::deque<Item>>();
        auto end2 = std::chrono::high_resolution_clock::now();
        for (auto pos = it.base(); pos != in_mem_data_log_queue_->end(); pos++)
        {
            new_in_mem_data_log_queue->emplace_back(std::move(*pos));
        }
        auto end3 = std::chrono::high_resolution_clock::now();
        auto ms3 =
            std::chrono::duration_cast<std::chrono::milliseconds>(end3 - end2);
        LOG(INFO) << "Copy rest of in mem state to new queue in " << ms3.count()
                  << " ms";
        affected =
            in_mem_data_log_queue_size - new_in_mem_data_log_queue->size();

        // clear old in mem state in background thread if affected is large,
        // cost handreds of ms
        if (affected >= ASYNC_PURGE_LOG_COUNT_THRESHOLD)
        {
            InMemoryLogStateToClear *sc =
                new InMemoryLogStateToClear(std::move(in_mem_data_log_queue_));
            bthread_t tid;
            bthread_start_background(
                &tid, NULL, AsyncClearInMemoryLogState, sc);
        }

        // replace in_mem_data_log_queue_ with a new one
        in_mem_data_log_queue_ = std::move(new_in_mem_data_log_queue);
        purge_start_idx_ = 0;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    LOG(INFO) << "Purged " << affected << " log items in mem state in "
              << ms.count() << " ms";
}

void LogStateRocksDBCloudImpl::AddLogItem(uint32_t cc_ng_id,
                                          uint64_t tx_number,
                                          uint64_t timestamp,
                                          const std::string &log_message)
{
    // Write to db cloud is db is not closed
    if (dbc_->IsOpened())
    {
        // Add both to mem state and cloud db state
        std::array<char, 20> key{};
        Serialize(key, timestamp, cc_ng_id, tx_number);
        auto status = dbc_->GetDBPtr()->Put(
            write_option_, rocksdb::Slice(key.data(), key.size()), log_message);
        if (!status.ok())
        {
            LOG(ERROR) << "add log item failed: " << status.ToString();
        }
        assert(status.ok());

        last_applied_data_tx_ = tx_number;
        std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
        log_count_before_purge_++;
        // purge in mem state if it's full by calling OnInMemStateFull
        if (log_count_before_purge_ >
                in_mem_data_log_queue_size_high_watermark_ &&
            !purging_in_mem_data_log_queue_)
        {
            purging_in_mem_data_log_queue_ = true;
            std::function<void(bool, uint64_t)> done =
                [this](bool succeed, uint64_t purged_log_count)
            {
                // reset purging_in_mem_data_log_queue_ to false after purge
                // finished
                DLOG(INFO) << "OnInMemStateFull callback";
                std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
                purging_in_mem_data_log_queue_ = false;
                // either purge succeed or not, reset
                // log_count_before_purge_, so unsucceeded snapshot will be
                // delayed for one more round
                log_count_before_purge_ =
                    log_count_before_purge_ - purged_log_count;
            };
            observer_->OnInMemStateFull(log_count_before_purge_,
                                        log_count_before_purge_ * sizeof(Item),
                                        std::move(done));
        }
    }
    else
    {
        // Add to mem state
        AddLogItemToMemState(cc_ng_id, tx_number, timestamp, log_message);
        last_applied_data_tx_ = tx_number;
    }
}

std::pair<bool, std::unique_ptr<ItemIterator>>
LogStateRocksDBCloudImpl::GetLogReplayList(uint32_t ng_id,
                                           uint64_t start_timestamp)
{
    std::unique_ptr<ItemIterator> result;

    // If db is rolling up, make a quick fail, let log_agent retry
    if (!mem_db_in_sync_.load(std::memory_order_acquire))
    {
        LOG(INFO)
            << "GetLogReplayList fail when rocksdb cloud is still rolling up";
        return std::make_pair(false, std::move(result));
    }

    std::vector<Item::Pointer> ddl_list;
    GetClusterScaleOpList(ddl_list);
    size_t scale_size = ddl_list.size();
    LOG(INFO) << "cluster_scale_list size: " << scale_size;

    GetSchemaOpList(ddl_list);
    size_t schema_size = ddl_list.size() - scale_size;
    LOG(INFO) << "schema_log_list size: " << schema_size;

    GetSplitRangeOpList(ddl_list);
    size_t rs_size = ddl_list.size() - scale_size - schema_size;
    LOG(INFO) << "split_range_op_list size: " << rs_size;

    result = std::make_unique<ItemIteratorRocksDBImpl>(rocksdb_scan_threads_,
                                                       std::move(ddl_list),
                                                       dbc_->GetDBPtr(),
                                                       start_timestamp,
                                                       ng_id);
    return std::make_pair(true, std::move(result));
}

std::pair<bool, Item::Pointer> LogStateRocksDBCloudImpl::SearchTxDataLog(
    uint64_t tx_number, uint32_t ng_id, uint64_t lower_bound_ts)
{
    if (!CheckOrWaitForMemDBInSync("SearchTxDataLog",
                                   cloud_config_.db_ready_timeout_us_))
    {
        return std::make_pair(false, nullptr);
    }

    LOG(INFO) << "log state search tx: " << tx_number << ", ng_id: " << ng_id
              << " lower_bound_ts: " << lower_bound_ts;

    // search tx log from last_ckpt_ts + 1
    uint64_t start_ts = 0;
    if (lower_bound_ts != 0)
    {
        start_ts = lower_bound_ts - 1;
    }
    else
    {
        start_ts = LastCkptTimestamp(ng_id);
        if (start_ts != 0)  // 0 indicates no checkpoint happened
        {
            start_ts += 1;
        }
    }
    LOG(INFO) << "iterate log records, ng: " << ng_id
              << ", start ts: " << start_ts;

    // iterate log records in range [start, limit)
    std::array<char, 20> start_key{};
    Serialize(start_key, start_ts, ng_id, 0);
    rocksdb::Slice start(start_key.data(), start_key.size());
    rocksdb::ReadOptions read_option;
    read_option.iterate_lower_bound = &start;
    auto it = dbc_->GetDBPtr()->NewIterator(read_option);
    std::array<char, 8> target_txn{};
    Item::Pointer ptr = nullptr;
    for (int idx = 0, shift = 56; shift >= 0; idx++, shift -= 8)
    {
        target_txn[idx] = (tx_number >> shift) & 0xff;
    }
    for (it->SeekToFirst(); it->Valid(); it->Next())
    {
        std::string_view key_sv = it->key().ToStringView();
        // key_sv is in form of timestamp(8) + ng_id(4) + tx_no(8)
        if (key_sv.compare(12, 8, target_txn.data(), 8) == 0)
        {
            uint32_t ng;
            uint64_t ts;
            uint64_t tx_no;
            Deserialize(it->key(), ts, ng, tx_no);
            LOG(INFO) << "Found matching key: ng_id: " << ng
                      << ", tx_no: " << tx_no << ", timestamp: " << ts;
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

void LogStateRocksDBCloudImpl::BeginSnapshot()
{
    LogState::MakeCopyOfNgInfoAndCatalogOps();
    snapshot_in_mem_data_log_queue_size_ = in_mem_data_log_queue_->size();
    snapshot_last_applied_data_tx_ = GetLastAppliedTx();
}

void LogStateRocksDBCloudImpl::CleanSnapshotState()
{
    LogState::CleanSnapshotState();
    snapshot_in_mem_data_log_queue_size_ = 0;
    snapshot_last_applied_data_tx_ = 0;
}

uint64_t LogStateRocksDBCloudImpl::GetLastAppliedTx()
{
    return last_applied_data_tx_;
}

uint64_t LogStateRocksDBCloudImpl::GetSnapshotLastAppliedTx()
{
    return snapshot_last_applied_data_tx_;
}

/**
 * read and load snapshot files.
 * files are in form of relative paths to snapshot_path
 */
int LogStateRocksDBCloudImpl::ReadSnapshot(
    const std::string &snapshot_path, const std::vector<std::string> &files)
{
    LOG(INFO) << "RocksDB Cloud state ReadSnapshot";

    // load nginfo and catalog
    {
        auto file_path = snapshot_path + "/nginfo_and_catalog";
        std::ifstream is(file_path.c_str());
        LoadNgInfoAndCatalogOpsFrom(is);
    }

    // load in mem data log
    {
        auto file_path = snapshot_path + "/in_mem_data_log";
        std::ifstream is(file_path.c_str());
        // load in mem state only if file exist, since leader snapshot won't
        // include in mem state
        if (is.good())
        {
            LoadSnapshotInMemState(is);
        }
    }

    return Start();
}

void LogStateRocksDBCloudImpl::LoadSnapshotInMemState(std::ifstream &is)
{
    uint32_t in_mem_log_size = 0;
    is.read(reinterpret_cast<char *>(&in_mem_log_size), sizeof(uint32_t));
    LOG(INFO) << "read snapshot in mem log state size: " << in_mem_log_size;
    for (uint32_t i = 0; i < in_mem_log_size; i++)
    {
        uint64_t tx_number = 0;
        uint64_t timestamp = 0;
        std::string log_message;
        size_t log_message_size = 0;
        uint32_t cc_ng_id = UINT32_MAX;
        is.read(reinterpret_cast<char *>(&tx_number), sizeof(uint64_t));
        is.read(reinterpret_cast<char *>(&timestamp), sizeof(uint64_t));
        is.read(reinterpret_cast<char *>(&log_message_size), sizeof(size_t));
        log_message.resize(log_message_size);
        is.read(log_message.data(), log_message_size);
        is.read(reinterpret_cast<char *>(&cc_ng_id), sizeof(uint32_t));
        Item item = Item(
            tx_number, timestamp, log_message, LogItemType::DataLog, cc_ng_id);
        in_mem_data_log_queue_->emplace_back(std::move(item));
        LOG(INFO) << "read snapshot in mem log tx_number: " << tx_number
                  << " timestamp: " << timestamp;
    }
}

/**
 * write snapshot files to snapshot_path and return the relative filenames
 */
std::vector<std::string> LogStateRocksDBCloudImpl::WriteSnapshot(
    const std::string &snapshot_path)
{
    DLOG(INFO) << "snapshot_path: " << snapshot_path;
    std::vector<std::string> res;
    {
        // write ng terms to file 'nginfo_and_catalog'
        {
            auto path = snapshot_path + "/nginfo_and_catalog";
            std::ofstream os(path.c_str());
            WriteSnapshotNgInfoAndCatalogOpsTo(os);
        }  // close file
        res.emplace_back("nginfo_and_catalog");

        {
            auto path = snapshot_path + "/in_mem_data_log";
            std::ofstream os(path.c_str());
            WriteSnapshotInMemState(os);
        }
        res.emplace_back("in_mem_data_log");
    }

    return res;
}

void LogStateRocksDBCloudImpl::WriteSnapshotInMemState(std::ofstream &os)
{
    LOG(INFO) << "write snapshot in mem log size : "
              << snapshot_in_mem_data_log_queue_size_;
    os.write(
        reinterpret_cast<const char *>(&snapshot_in_mem_data_log_queue_size_),
        sizeof(uint32_t));

    if (snapshot_in_mem_data_log_queue_size_ == 0)
    {
        return;
    }

    auto start = std::chrono::high_resolution_clock::now();
    std::unique_lock<bthread::Mutex> lk(in_mem_state_mutex_);
    size_t cnt = 0;
    for (std::deque<Item>::iterator it = in_mem_data_log_queue_->begin();
         it != in_mem_data_log_queue_->end();
         ++it)
    {
        const uint64_t &tx_number = it->tx_number_;
        const uint64_t &timestamp = it->timestamp_;
        const std::string &log_message = it->log_message_;
        size_t log_message_size = log_message.size();
        uint32_t cc_ng_id = it->cc_ng_;
        os.write(reinterpret_cast<const char *>(&tx_number), sizeof(uint64_t));
        os.write(reinterpret_cast<const char *>(&timestamp), sizeof(uint64_t));
        os.write(reinterpret_cast<const char *>(&log_message_size),
                 sizeof(size_t));
        os.write(log_message.data(), log_message_size);
        os.write(reinterpret_cast<const char *>(&cc_ng_id), sizeof(uint32_t));
        cnt++;
        if (cnt == snapshot_in_mem_data_log_queue_size_)
        {
            break;
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    LOG(INFO) << "write snapshot in mem log in " << ms.count() << " ms";
}

/**
 * Start log state.
 * Called from two places: when log instance starts and when loading snapshot.
 * Moreover, Start will be called twice if an instance crash and recover:
 * first in starting braft::node which calls
 * braft::StateMachine::on_snapshot_load, then in starting LogState.
 * check db_ to avoid executing rocksdb::DB::Open twice.
 */
int LogStateRocksDBCloudImpl::Start()
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

    return 0;
}

void LogStateRocksDBCloudImpl::StopRocksDB()
{
    // release DBCloudConatiner, then internal DBCloud will be closed if ref
    // count to zero
    {
        std::unique_lock<bthread::Mutex> lk(mem_db_in_sync_mutex_);
        mem_db_in_sync_.store(false, std::memory_order_release);
    }

    mem_db_in_sync_cv_.notify_all();
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }
    LOG(INFO) << "RocksDB Cloud stopped";
}

void LogStateRocksDBCloudImpl::WaitForAsyncStartCloudDBFinishIfAny()
{
    if (cloud_db_init_thread_.joinable())
    {
        cloud_db_init_thread_.join();
    }
}

void LogStateRocksDBCloudImpl::AsyncStartCloudDB(int64_t old_term,
                                                 int64_t new_term)
{
    LOG(INFO) << "Start RocksDB Cloud Async";

    // this dbc instance will serve the leader
    {
        std::unique_lock<bthread::Mutex> lk(dbc_mutex_);
        dbc_ = std::make_shared<DBCloudContainer>();
    }

    // wait prevous cloud db run to finish
    WaitForAsyncStartCloudDBFinishIfAny();

    cloud_db_init_thread_ = std::thread(
        [this, old_term, new_term, dbc_async_start(dbc_)]
        {
            LOG(INFO) << "RocksDB Cloud Init Thread start to work, old_term: "
                      << old_term << " ,new_term: " << new_term;

            CODE_FAULT_INJECTOR("new_leader_dont_start_rocksdb_cloud", {
                LOG(INFO) << "FaultInject triggered, "
                             "new_leader_dont_start_rocksdb_cloud";
                return;
            });

            LOG(INFO) << "Opening RocksDB Cloud";
            auto start = std::chrono::high_resolution_clock::now();
            CODE_FAULT_INJECTOR("new_leader_sleep_when_start_rocksdb_cloud", {
                LOG(INFO) << "FaultInject triggered, "
                             "new_leader_sleep_when_start_rocksdb_cloud";
                sleep(10);
            });

            // cloud fs config
            rocksdb::CloudFileSystemOptions cfs_options;

#if (USE_ROCKSDB_CLOUD == CS_TYPE_S3)
            if (cloud_config_.aws_access_key_id_.length() == 0 ||
                cloud_config_.aws_secret_key_.length() == 0)
            {
                LOG(INFO) << "No AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                             "provided, use default credential provider";
                cfs_options.credentials.type =
                    rocksdb::AwsAccessType::kUndefined;
            }
            else
            {
                cfs_options.credentials.InitializeSimple(
                    cloud_config_.aws_access_key_id_,
                    cloud_config_.aws_secret_key_);
            }

            status = cfs_options.credentials.HasValid();
            if (!status.ok())
            {
                LOG(ERROR) << "Valid AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY "
                              "is required, error: "
                           << status.ToString();
                std::abort();
            }
#endif

            cfs_options.src_bucket.SetBucketName(cloud_config_.bucket_name_,
                                                 cloud_config_.bucket_prefix_);
            cfs_options.src_bucket.SetRegion(cloud_config_.region_);
            cfs_options.src_bucket.SetObjectPath("rocksdb_cloud");
            cfs_options.dest_bucket.SetBucketName(cloud_config_.bucket_name_,
                                                  cloud_config_.bucket_prefix_);
            cfs_options.dest_bucket.SetRegion(cloud_config_.region_);
            cfs_options.dest_bucket.SetObjectPath("rocksdb_cloud");
            // Add sst_file_cache for accerlating random access on sst files
            cfs_options.sst_file_cache =
                rocksdb::NewLRUCache(cloud_config_.sst_file_cache_size_);
            // delay cloud file deletion for 1 hour
            cfs_options.cloud_file_deletion_delay =
                std::chrono::seconds(cloud_config_.db_file_deletion_delay_);

            // Get the cookie on open from the previous term
            rocksdb::CloudFileSystem *cfs;
            auto status = NewCloudFileSystem(cfs_options, &cfs);

            if (!status.ok())
            {
                LOG(ERROR)
                    << "Unable to create cloud storage filesystem, cloud type: "
#if (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)
                    << "aws"
#elif (WITH_ROCKSDB_CLOUD == CS_TYPE_GCS)
                    << "gcp"
#endif
                    << ", at path rocksdb_cloud with bucket "
                    << cloud_config_.bucket_name_ << " prefix "
                    << cloud_config_.bucket_prefix_
                    << ", with error: " << status.ToString();

                std::abort();
            }

            int64_t tmp_old_term = old_term;
            std::string cookie_on_open = "";
            std::string new_cookie_on_open = std::to_string(new_term);
            // find the latest cloud manifest file
            if (tmp_old_term >= 0)
            {
                auto storage_provider = cfs->GetStorageProvider();
                while (tmp_old_term >= 0)
                {
                    std::string cloud_manifest_file_name =
                        MakeCloudManifestFile("rocksdb_cloud",
                                              std::to_string(tmp_old_term));
                    auto st = storage_provider->ExistsCloudObject(
                        cloud_config_.bucket_prefix_ +
                            cloud_config_.bucket_name_,
                        cloud_manifest_file_name);
                    if (st.ok())
                    {
                        LOG(INFO) << "Latest RocksDB Cloud Manifest file "
                                  << cloud_manifest_file_name << " found";
                        cookie_on_open = std::to_string(tmp_old_term);
                        break;
                    }
                    else
                    {
                        LOG(ERROR) << "RocksDB Cloud Manifest file "
                                   << cloud_manifest_file_name << " not found";
                        tmp_old_term--;
                    }
                }
            }

            // destroy the cfs after checking the cookie on open
            delete cfs;

            // new CLOUDMANIFEST suffixed by cookie and epochID suffixed
            // MANIFEST files are generated, which won't overwrite the old ones
            // opened by previous leader
            cfs_options.cookie_on_open = cookie_on_open;
            cfs_options.new_cookie_on_open = new_cookie_on_open;
            // delay cloud file deletion for 1 hour
            cfs_options.cloud_file_deletion_delay =
                std::chrono::seconds(cloud_config_.db_file_deletion_delay_);
            // use aws transfer manager to upload/download files
            /* cfs_options.use_aws_transfer_manager = true; Don't use!!! it will
             * cause corrupt file download*/
            // sync cloudmanifest and manifest files when open db
            cfs_options.resync_on_open = true;

            // start db cloud in DBCLoudContainer and dump in memory log items
            dbc_async_start->Open(cfs_options,
                                  rocksdb_storage_path_,
                                  rocksdb_max_write_buffer_number_,
                                  rocksdb_max_background_jobs_,
                                  rocksdb_target_file_size_base_);

            auto stop = std::chrono::high_resolution_clock::now();
            uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              stop - start)
                              .count();
            LOG(INFO) << "Open RocksDB Cloud costs " << ms << " milliseconds";

            uint64_t max_file_number =
                dbc_async_start->GetDBPtr()->GetNextFileNumber() - 1;
            LOG(INFO) << "The max_file_number after DBCloud open: "
                      << max_file_number;

            // set max file number after open the db
            SetMaxFileNumberAfterLatestFlush(max_file_number);

            // sync in memory state into db
            start = std::chrono::high_resolution_clock::now();

            // sync in memory state into db, the in_mem_data_log_queue_ won't be
            // changed after db is opened, so we can safely iterate it.
            // if leader changed again, we wait for this async start thread to
            // finish in on_stop_following, so that the in_mem_data_log_queue_
            // won't be changed after db is opened
            int64_t cnt = 0;

            CODE_FAULT_INJECTOR("disable_in_mem_state_to_db_sync", {
                LOG(INFO) << "FaultInject triggered, "
                             "disable_in_mem_state_to_db_sync";
                goto mem_db_in_sync;
            });

            {
                std::unique_lock<bthread::Mutex> in_mem_state_lk(
                    in_mem_state_mutex_);
                for (const auto &item : *in_mem_data_log_queue_)
                {
                    std::array<char, 20> key{};
                    uint32_t cc_ng_id = item.cc_ng_;
                    Serialize(key, item.timestamp_, cc_ng_id, item.tx_number_);
                    auto status = dbc_async_start->GetDBPtr()->Put(
                        write_option_,
                        rocksdb::Slice(key.data(), key.size()),
                        item.log_message_);
                    cnt++;
                    if (!status.ok())
                    {
                        LOG(ERROR)
                            << "add log item failed: " << status.ToString();
                        std::abort();
                    }
                }

                in_mem_data_log_queue_->clear();
                purge_start_idx_ = 0;
            }

        mem_db_in_sync:
        {
            std::unique_lock<bthread::Mutex> lk(mem_db_in_sync_mutex_);
            // update mem_db_in_sync_ only when term still matched, in case
            // of in middle step down during async start db
            if (new_term ==
                term_if_is_lg_leader_.load(std::memory_order_acquire))
            {
                mem_db_in_sync_.store(true, std::memory_order_release);
            }
            else
            {
                LOG(INFO) << "lg leader changed during async start "
                             "roccksdb cloud since term mismatch, term="
                          << new_term << ", current_term="
                          << term_if_is_lg_leader_.load(
                                 std::memory_order_acquire);
            }
        }
            mem_db_in_sync_cv_.notify_all();

            stop = std::chrono::high_resolution_clock::now();
            ms = std::chrono::duration_cast<std::chrono::milliseconds>(stop -
                                                                       start)
                     .count();
            LOG(INFO) << "Dump " << cnt
                      << " in memory log items into RocksDB Cloud, costs " << ms
                      << " millseconds";
        });
}

bool LogStateRocksDBCloudImpl::RefillInMemStateFromCloudDB(
    std::shared_ptr<DBCloudContainer> dbc,
    uint64_t start_file_number,
    uint64_t end_file_number)
{
    if (start_file_number == end_file_number)
    {
        LOG(INFO) << "No need to refill in mem state from RocksDB Cloud, "
                     "start_sst_number == "
                  << start_file_number
                  << " ,end_sst_number == " << end_file_number;
        return true;
    }
    std::lock_guard<bthread::Mutex> lk(in_mem_state_mutex_);

    auto options = dbc->GetDBPtr()->GetOptions();
    auto fs = options.env->GetFileSystem();
    auto start = std::chrono::high_resolution_clock::now();
    for (auto file_number = start_file_number + 1;
         file_number <= end_file_number;
         file_number++)
    {
        std::string file_name =
            ROCKSDB_NAMESPACE::MakeTableFileName(file_number);
        // check if the file exists
        // the file number may be used by other file type instead of sst file,
        // so we need to check if the file exists, if not, skip this file and
        // continue
        rocksdb::SstFileReader sst_reader(options);
        rocksdb::Status status = sst_reader.Open(file_name);
        if (!status.ok())
        {
            LOG(WARNING) << "Can't open sst file: " << file_name
                         << ", error: " << status.ToString()
                         << ", the file number " << file_number
                         << " maybe used by other file type, skip this file "
                            "and continue";
            continue;
        }
        status = sst_reader.VerifyChecksum();
        if (!status.ok())
        {
            LOG(ERROR)
                << "Refill in mem state failed, due to verify checksum failed: "
                << status.ToString();
            return false;
        }
        rocksdb::ReadOptions read_options;
        rocksdb::Iterator *it = sst_reader.NewIterator(read_options);
        if (!it->status().ok())
        {
            LOG(ERROR)
                << "Refill in mem state failed, due to new iterator failed: "
                << it->status().ToString();
            return false;
        }

        for (it->SeekToFirst(); it->Valid(); it->Next())
        {
            rocksdb::Slice key = it->key();
            rocksdb::Slice value = it->value();
            uint64_t timestamp, tx_number;
            uint32_t ng_id;
            Deserialize(key, timestamp, ng_id, tx_number);
            Item item;
            item.tx_number_ = tx_number;
            item.timestamp_ = timestamp;
            item.log_message_ = {value.data(), value.size()};
            item.cc_ng_ = ng_id;
            item.item_type_ = LogItemType::DataLog;
            in_mem_data_log_queue_->emplace_front(std::move(item));
        }
    }

    size_t log_cnt = in_mem_data_log_queue_->size();
    purge_start_idx_ = log_cnt == 0 ? 0 : log_cnt - 1;
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    LOG(INFO) << "Refill in memory state form RocksDB Cloud, log count: "
              << log_cnt << " ,duration: " << duration.count()
              << " millseconds";
    return true;
}

uint64_t LogStateRocksDBCloudImpl::GetApproximateReplayLogSize()
{
    if (!dbc_->IsOpened())
    {
        return 0;
    }

    uint64_t total_memtable_size = 0;
    uint64_t total_sst_size = 0;

    /* Get total memtables size */
    std::string size_val;
    if (dbc_->GetDBPtr()->GetProperty("rocksdb.cur-size-all-mem-tables",
                                      &size_val))
    {
        total_memtable_size = std::stoi(size_val);
    }
    else
    {
        LOG(ERROR) << "Failed to get memory table size";
    }

    /* Get total SStables size used in Tx recovery  */
    std::vector<rocksdb::LiveFileMetaData> metadata;
    dbc_->GetDBPtr()->GetLiveFilesMetaData(&metadata);
    for (const auto &meta : metadata)
    {
        rocksdb::Slice largestkey(meta.largestkey);
        uint64_t lk_ts, lk_tx_no;
        uint32_t lk_ng_id;
        Deserialize(largestkey, lk_ts, lk_ng_id, lk_tx_no);

        if (min_ckpt_ts_ <= lk_ts)
        {
            DLOG(INFO) << "SSTable " << meta.name
                       << " used in replay with size: "
                       << FormatSize(meta.size);
            total_sst_size += meta.size;
        }
    }

    LOG(INFO) << "Total memtables size: " << FormatSize(total_memtable_size);
    LOG(INFO) << "Total SSTables size used in replay: "
              << FormatSize(total_sst_size);

    uint64_t total_txlog_size = total_memtable_size + total_sst_size;
    LOG(INFO) << "Total replay log size: " << FormatSize(total_txlog_size);
    return total_txlog_size;
}

/**
 * for debug use only
 */
void LogStateRocksDBCloudImpl::PrintKey(rocksdb::Slice key)
{
    uint32_t ng;
    uint64_t timestamp, tx_no;
    Deserialize(key, timestamp, ng, tx_no);
    LOG(INFO) << "ng_id: " << ng << ",\ttimestamp: " << timestamp
              << ",\ttx_number: " << tx_no;
}
}  // namespace txlog
#endif