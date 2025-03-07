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
    stop_worker_.store(true, std::memory_order_release);

    // Notify all waiting threads to check the stop flag
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
            for (size_t i = 0; i < count; ++i)
            {
                ProcessTask(tasks[i]);
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
        for (auto it = data_log.node_txn_logs().begin();
             it != data_log.node_txn_logs().end();
             ++it)
        {
            err = log_state_->AddLogItem(txn, timestamp, it->second);
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

    task_cnt_.fetch_add(1, std::memory_order_relaxed);
    queue_cv_.notify_one();
}

}  // namespace txlog