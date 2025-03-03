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
#include <bthread/mutex.h>

#include "log.pb.h"
#include "log_state.h"

namespace txlog
{
/*
 * RaftLogClosure is the closure used by raft log state machine
 *
 * Braft node call `apply` method to execute a task asynchronously. When the
 * task is finished, `on_apply` will call RaftLogClosure->Run() with the help
 * of brpc::ClosureGuard or braft::AsyncClosureGuard.
 *
 * Note that RaftLogClosure is also a google::protobuf::Closure.
 */
class RaftLogClosure : public braft::Closure
{
public:
    explicit RaftLogClosure(const LogRequest *request,
                            LogResponse *response,
                            bool *finish,
                            bthread::Mutex *mu,
                            bthread::ConditionVariable *cv)
        : request_(request),
          response_(response),
          finish_(finish),
          mu_(mu),
          cv_(cv)
    {
    }
    ~RaftLogClosure()
    {
    }

    const LogRequest *request() const
    {
        return request_;
    }

    LogResponse *response() const
    {
        return response_;
    }

    /*
     * Run() is the callback when raft closure is done.
     */
    void Run() override
    {
        // raft apply() fail will enter the RaftLogClosure's Run(). But apply()
        // fails doesn't means the operation of apply log fails, there exists
        // false negative case during leader transfer. Hence we should return
        // Unknown status to client.
        if (!status().ok())
        {
            response_->set_response_status(
                LogResponse::ResponseStatus::
                    LogResponse_ResponseStatus_Unknown);
        }

        std::unique_lock lk(*mu_);
        *finish_ = true;

        cv_->notify_one();
    }

private:
    const LogRequest *request_;
    LogResponse *response_;

    bool *finish_;
    bthread::Mutex *mu_;
    bthread::ConditionVariable *cv_;
};
}  // namespace txlog
