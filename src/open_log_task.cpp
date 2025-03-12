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
#include "open_log_task.h"

#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "log_state.h"
#include "open_log_service.h"

namespace txlog
{
OpenLogTaskWorker::OpenLogTaskWorker(size_t num_threads) : stop_worker_(false)
{
    for (size_t i = 0; i < num_threads; ++i)
    {
        worker_threads_.emplace_back(&OpenLogTaskWorker::WorkerThreadMain,
                                     this);
    }
}

OpenLogTaskWorker::~OpenLogTaskWorker()
{
    Shutdown();
}

void OpenLogTaskWorker::Shutdown()
{
    {
        // Even if the shared variable is atomic, it must be modified while
        // owning the mutex to correctly publish the modification to the waiting
        // thread. (https://en.cppreference.com/w/cpp/thread/condition_variable)
        std::unique_lock<std::mutex> lk(queue_mutex_);
        stop_worker_.store(true, std::memory_order_release);
    }
    queue_cv_.notify_all();

    for (auto &thread : worker_threads_)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
}

void OpenLogTaskWorker::WorkerThreadMain()
{
    constexpr size_t MAX_BATCH_SIZE = 16;
    OpenLogServiceTask *tasks[MAX_BATCH_SIZE];

    while (!stop_worker_.load(std::memory_order_relaxed))
    {
        size_t count = task_queue_.try_dequeue_bulk(tasks, MAX_BATCH_SIZE);

        if (count > 0)
        {
            task_cnt_.fetch_sub(count, std::memory_order_relaxed);

            // First pass: collect all data log writes from across tasks
            std::vector<std::tuple<uint64_t, uint64_t, std::string>> batch_logs;
            // Keep track of which tasks have data logs to update their
            // responses later
            std::vector<std::pair<OpenLogServiceTask *, size_t>> data_log_tasks;

            // First pass: identify data log tasks and collect writes
            for (size_t i = 0; i < count; ++i)
            {
                OpenLogServiceTask *task = tasks[i];
                const LogRequest *req = task->req_;

                // Only batch kDataLog requests
                if (req->request_case() ==
                        LogRequest::RequestCase::kWriteLogRequest &&
                    req->write_log_request().log_content().content_case() ==
                        LogContentMessage::ContentCase::kDataLog)
                {
                    const WriteLogRequest &write_req = req->write_log_request();
                    const uint64_t timestamp = write_req.commit_timestamp();
                    uint64_t txn = write_req.txn_number();
                    const DataLogMessage &data_log =
                        write_req.log_content().data_log();

                    // Record where this task's data logs start in batch_logs
                    size_t start_index = batch_logs.size();
                    data_log_tasks.emplace_back(task, start_index);

                    // Add all log entries from this task to the batch
                    for (auto it = data_log.node_txn_logs().begin();
                         it != data_log.node_txn_logs().end();
                         ++it)
                    {
                        batch_logs.emplace_back(txn, timestamp, it->second);
                    }
                }
            }

            // Perform batch write if there are data logs
            int batch_err = 0;
            if (!batch_logs.empty())
            {
                batch_err = log_state_->AddLogItemBatch(batch_logs);
            }

            // Second pass: process all tasks and set responses
            for (size_t i = 0; i < count; ++i)
            {
                OpenLogServiceTask *task = tasks[i];
                const LogRequest *req = task->req_;
                LogResponse *resp = task->resp_;

                // Check if this is a data log task we've already batched
                bool is_batched_data_log = false;
                for (const auto &[dl_task, start_index] : data_log_tasks)
                {
                    if (dl_task == task)
                    {
                        is_batched_data_log = true;

                        // For batched data logs, use the batch error code
                        if (batch_err != 0)
                        {
                            OpenLogServiceImpl::SetWriteLogErrorResponse(
                                req->write_log_request(), resp);
                        }
                        else
                        {
                            resp->set_response_status(
                                LogResponse::ResponseStatus::
                                    LogResponse_ResponseStatus_Success);
                        }

                        // Update latest committed transaction number
                        uint32_t tx_ident =
                            req->write_log_request().txn_number() & 0xFFFFFFFF;
                        log_state_->UpdateLatestCommittedTxnNumber(tx_ident);
                        break;
                    }
                }

                // Process non-data log tasks normally
                if (!is_batched_data_log)
                {
                    ProcessTask(task);
                }
                else
                {
                    // For batched tasks, we still need to notify when complete
                    std::unique_lock<bthread::Mutex> lk(task->mux_);
                    task->finished_ = true;
                    task->cv_.notify_one();
                }
            }
        }
        else
        {
            // Wait for a task to be enqueued or for shutdown signal
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(
                lock,
                [this]()
                {
                    return stop_worker_.load(std::memory_order_relaxed) ||
                           task_cnt_.load(std::memory_order_relaxed) > 0;
                });
        }
    }
}

void OpenLogTaskWorker::ProcessTask(OpenLogServiceTask *task)
{
    const LogRequest *req = task->req_;
    LogResponse *resp = task->resp_;

    switch (req->request_case())
    {
    case LogRequest::RequestCase::kWriteLogRequest:
    {
        HandleWriteLog(req->write_log_request(), resp);
        break;
    }
    case txlog::LogRequest::RequestCase::kUpdateCkptTsRequest:
    {
        HandleUpdateCkptTs(req->update_ckpt_ts_request(), resp);
        break;
    }
    default:
        break;
    }

    std::unique_lock<bthread::Mutex> lk(task->mux_);
    task->finished_ = true;
    task->cv_.notify_one();
}

void OpenLogTaskWorker::HandleWriteLog(const WriteLogRequest &req,
                                       LogResponse *resp)
{
    const uint64_t timestamp = req.commit_timestamp();
    uint64_t txn = req.txn_number();
    int err = 0;

    const LogContentMessage &log_content = req.log_content();
    switch (log_content.content_case())
    {
    case LogContentMessage::ContentCase::kDataLog:
    {
        const DataLogMessage &data_log = log_content.data_log();

        // Process individual data log entries - batching is now done at
        // WorkerThreadMain level
        for (auto it = data_log.node_txn_logs().begin();
             it != data_log.node_txn_logs().end();
             ++it)
        {
            err = log_state_->AddLogItem(txn, timestamp, it->second);
            if (err != 0)
                break;
        }
        break;
    }
    case LogContentMessage::ContentCase::kSchemaLog:
    {
        const SchemaOpMessage &schema_log = log_content.schema_log();
        log_state_->UpdateSchemaOp(txn, timestamp, schema_log);
        break;
    }
    case LogContentMessage::ContentCase::kSplitRangeLog:
    {
        const SplitRangeOpMessage &split_range_op_message =
            log_content.split_range_log();
        log_state_->UpdateSplitRangeOp(txn, timestamp, split_range_op_message);
        break;
    }
    default:
        break;
    }

    uint32_t tx_ident = req.txn_number() & 0xFFFFFFFF;
    log_state_->UpdateLatestCommittedTxnNumber(tx_ident);

    if (err != 0)
    {
        OpenLogServiceImpl::SetWriteLogErrorResponse(req, resp);
    }
    else
    {
        resp->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success);
    }
}

void OpenLogTaskWorker::HandleUpdateCkptTs(const UpdateCheckpointTsRequest &req,
                                           LogResponse *resp)
{
    log_state_->UpdateCkptTs(req.ckpt_timestamp());
    resp->set_response_status(
        LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success);
}

void OpenLogTaskWorker::EnqueueTask(OpenLogServiceTask *task)
{
    task_queue_.enqueue(task);

    {
        // Even if the shared variable is atomic, it must be modified while
        // owning the mutex to correctly publish the modification to the waiting
        // thread.(https://en.cppreference.com/w/cpp/thread/condition_variable)
        std::unique_lock<std::mutex> lk(queue_mutex_);
        task_cnt_.fetch_add(1, std::memory_order_relaxed);
    }
    queue_cv_.notify_one();
}

}  // namespace txlog