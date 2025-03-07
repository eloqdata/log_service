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

#include <bthread/condition_variable.h>
#include <bthread/moodycamelqueue.h>
#include <bthread/mutex.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "log.pb.h"

namespace txlog
{
class LogState;

struct OpenLogServiceTask
{
    OpenLogServiceTask(const LogRequest *req, LogResponse *resp)
        : req_(req), resp_(resp)
    {
    }
    const LogRequest *req_;
    LogResponse *resp_;
    bthread::Mutex mux_;
    bthread::ConditionVariable cv_;
    bool finished_{false};
};

class OpenLogTaskWorker
{
public:
    OpenLogTaskWorker(size_t num_threads = 4);
    ~OpenLogTaskWorker();
    void EnqueueTask(OpenLogServiceTask *task);
    void SetLogState(LogState *log_state)
    {
        log_state_ = log_state;
    }
    void Shutdown();

private:
    void WorkerThreadMain();
    void ProcessTask(OpenLogServiceTask *task);
    void HandleWriteLog(const WriteLogRequest &req, LogResponse *resp);
    void HandleUpdateCkptTs(const UpdateCheckpointTsRequest &req,
                            LogResponse *resp);

    moodycamel::ConcurrentQueue<OpenLogServiceTask *> task_queue_;
    std::vector<std::thread> worker_threads_;
    std::atomic<bool> stop_worker_;
    LogState *log_state_;

    // Added for condition variable implementation
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<uint32_t> task_cnt_{0};
};
}  // namespace txlog