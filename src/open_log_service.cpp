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
#include "open_log_service.h"

#include <brpc/closure_guard.h>
#include <butil/logging.h>

namespace txlog
{
thread_local size_t OpenLogServiceImpl::received_task_cnt_ = 0;

void OpenLogServiceImpl::WriteLog(::google::protobuf::RpcController *controller,
                                  const LogRequest *request,
                                  LogResponse *response,
                                  ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    // Computes the log group ID from the tx ID.
    const WriteLogRequest &log_req = request->write_log_request();

    // The Write log caller determine the log group
    uint32_t log_group_id = log_req.log_group_id();
    if (log_group_id != log_group_id_)
    {
        LOG(ERROR) << "A write log request toward the log #" << log_group_id
                   << " is directed to a log node that does not match the log "
                      "group ID.";
        SetWriteLogErrorResponse(log_req, response);
        return;
    }

    SubmitAndWait(request, response);
}

void OpenLogServiceImpl::ReplayLog(
    ::google::protobuf::RpcController *controller,
    const LogRequest *request,
    LogResponse *response,
    ::google::protobuf::Closure *done)
{
    LOG(INFO) << "Received ReplayLogRequest.";

    brpc::ClosureGuard done_guard(done);

    const ReplayLogRequest &req = request->replay_log_request();
    const uint32_t cc_ng_id = req.cc_node_group_id();

    const std::string &cc_node_ip = req.source_ip();
    uint16_t cc_node_port = req.source_port();

    uint64_t last_ckpt_ts = log_state_->LastCkptTimestamp();
    // get log list since last_ckpt_ts+1
    // 0 indicates no checkpoint happened
    uint64_t start_ts = last_ckpt_ts == 0 ? 0 : last_ckpt_ts + 1;
    if (req.start_ts() != 0)
    {
        start_ts = std::min(start_ts, req.start_ts());
    }
    LOG(INFO) << "Start replaying from timestamp " << start_ts;

    auto [ok, iterator] = log_state_->GetLogReplayList(start_ts);

    if (!ok)
    {
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
        return;
    }

    uint32_t latest_txn_no = log_state_->LatestCommittedTxnNumber();

    std::unique_lock<std::mutex> lk(log_replay_workers_mutex_);

    // The new log shipping agent spawns a background thread that ships log
    // records to the recovering leader of the cc node group. When there is
    // already a shipping agent for the specified node group, either active or
    // inactive, replaces the old shipping agent with a new one. The old agent
    // may still be active, when the old leader has not finished failover before
    // transferring the leadership to a new cc node. The replacement will
    // interrupt and de-allocate the old shipping agent.
    log_replay_worker_ = std::make_unique<LogShippingAgent>(log_group_id_,
                                                            cc_ng_id,
                                                            DEFAULT_CC_NG_TERM,
                                                            cc_node_ip,
                                                            cc_node_port,
                                                            std::move(iterator),
                                                            latest_txn_no,
                                                            last_ckpt_ts,
                                                            true);

    assert(response != nullptr);
    response->set_response_status(
        LogResponse::ResponseStatus::LogResponse_ResponseStatus_Success);
    LOG(INFO) << "ReplayLogRequest succeeds.";
}

void OpenLogServiceImpl::UpdateCheckpointTs(
    ::google::protobuf::RpcController *controller,
    const LogRequest *request,
    LogResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);
    SubmitAndWait(request, response);
}

void OpenLogServiceImpl::RecoverTx(
    ::google::protobuf::RpcController *controller,
    const ::txlog::RecoverTxRequest *request,
    ::txlog::RecoverTxResponse *response,
    ::google::protobuf::Closure *done)
{
    brpc::ClosureGuard done_guard(done);

    if (log_group_id_ != request->log_group_id())
    {
        response->set_tx_status(::txlog::RecoverTxResponse_TxStatus::
                                    RecoverTxResponse_TxStatus_RecoverError);
        return;
    }
    uint32_t participant_cc_ng_id = request->cc_node_group_id();
    int64_t dest_node_term = DEFAULT_CC_NG_TERM;

    auto [success, record] = log_state_->SearchTxDataLog(
        request->lock_tx_number(), request->write_lock_ts());

    if (!success)
    {
        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_RecoverError);
        return;
    }

    bool has_multi_stage_log =
        log_state_->SearchTxSchemaLog(request->lock_tx_number()).first;

    if (!has_multi_stage_log)
    {
        has_multi_stage_log =
            log_state_->SearchTxSplitRangeOp(request->lock_tx_number()).first;
    }

    if (record == nullptr && !has_multi_stage_log)
    {
        // No log record found for the specified tx. The tx is deemed aborted.
        response->set_tx_status(
            ::txlog::RecoverTxResponse_TxStatus_NotCommitted);
        return;
    }

    // Sets the tx status to committed. For the DML tx, the txn log record will
    // be shipped to the cc node asynchronously and the lock will be cleared
    // when the log record is replayed. For multi-stage txn like DDL and range
    // split, since it has written the log, the txn is guaranteed to succeed and
    // release all the locks.
    response->set_tx_status(::txlog::RecoverTxResponse_TxStatus_Committed);
    if (record != nullptr)
    {
        LogShippingAgent *ship_agent = nullptr;

        std::lock_guard guard(log_replay_workers_mutex_);
        if (log_replay_worker_ == nullptr)
        {
            log_replay_worker_ = std::make_unique<LogShippingAgent>(
                log_group_id_,
                participant_cc_ng_id,
                dest_node_term,
                request->source_ip(),
                request->source_port(),
                std::unique_ptr<ItemIterator>{},
                0,
                0,
                false);
        }
        ship_agent = log_replay_worker_.get();

        ship_agent->AddLogRecord(record);
    }
}

void OpenLogServiceImpl::SetWriteLogErrorResponse(
    const WriteLogRequest &request, LogResponse *response)
{
    if (request.retry())
    {
        // should return unknown status when retry write log request is
        // forwarded to a follower node. Client should resend the request to
        // the new leader.
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Unknown);
    }
    else
    {
        DLOG(INFO)
            << "Log service: Write log request encounter an error, tx_number:"
            << request.txn_number();
        response->set_response_status(
            LogResponse::ResponseStatus::LogResponse_ResponseStatus_Fail);
    }
}

void OpenLogServiceImpl::SubmitAndWait(const LogRequest *req, LogResponse *resp)
{
    OpenLogServiceTask task{req, resp};

    assert(!workers_.empty());
    size_t idx = received_task_cnt_ % workers_.size();
    ++received_task_cnt_;
    workers_[idx]->EnqueueTask(&task);

    std::unique_lock<bthread::Mutex> lk(task.mux_);
    while (!task.finished_)
    {
        task.cv_.wait(lk);
    }
}

int OpenLogServiceImpl::Start(LogState *log_state, uint32_t worker_concurrency)
{
    log_state_ = log_state;
    if (log_state_->Start())
    {
        LOG(ERROR) << "Fail to start log_state";
        return -1;
    }

    for (uint32_t i = 0; i < worker_concurrency; ++i)
    {
        workers_.emplace_back(std::make_unique<OpenLogTaskWorker>());
        workers_.back()->SetLogState(log_state);
    }

    return 0;
}

void OpenLogServiceImpl::Shutdown()
{
    for (auto &worker : workers_)
    {
        worker->Shutdown();
    }
    workers_.clear();
    LOG(INFO) << "OpenLogTaskWorkers stopped and deallocated.";
}
}  // namespace txlog