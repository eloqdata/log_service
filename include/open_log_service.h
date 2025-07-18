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

#include "json2pb/pb_to_json.h"
#include "log.pb.h"
#include "log_shipping_agent.h"
#include "log_state.h"
#include "open_log_task.h"

namespace txlog
{
class OpenLogServiceImpl : public LogService
{
public:
    OpenLogServiceImpl(uint32_t log_group_id) : log_group_id_(log_group_id)
    {
    }

    void WriteLog(::google::protobuf::RpcController *controller,
                  const LogRequest *request,
                  LogResponse *response,
                  ::google::protobuf::Closure *done) override;

    void AttachLog(::google::protobuf::RpcController *controller,
                   const ::txlog::AttachLogRequest *request,
                   ::txlog::AttachLogResponse *response,
                   ::google::protobuf::Closure *done) override
    {
    }

    void DetachLog(::google::protobuf::RpcController *controller,
                   const ::google::protobuf::Empty *request,
                   ::google::protobuf::Empty *response,
                   ::google::protobuf::Closure *done) override
    {
    }

    void CheckClusterScaleStatus(::google::protobuf::RpcController *controller,
                                 const LogRequest *request,
                                 LogResponse *response,
                                 ::google::protobuf::Closure *done) override
    {
    }

    void CheckMigrationIsFinished(
        ::google::protobuf::RpcController *controller,
        const ::txlog::CheckMigrationIsFinishedRequest *request,
        ::txlog::CheckMigrationIsFinishedResponse *response,
        ::google::protobuf::Closure *done) override
    {
    }

    void ReplayLog(::google::protobuf::RpcController *controller,
                   const LogRequest *request,
                   LogResponse *response,
                   ::google::protobuf::Closure *done) override;

    void UpdateCheckpointTs(::google::protobuf::RpcController *controller,
                            const LogRequest *request,
                            LogResponse *response,
                            ::google::protobuf::Closure *done) override;

    void RecoverTx(::google::protobuf::RpcController *controller,
                   const ::txlog::RecoverTxRequest *request,
                   ::txlog::RecoverTxResponse *response,
                   ::google::protobuf::Closure *done) override;

    void TransferLeader(::google::protobuf::RpcController *controller,
                        const ::txlog::TransferRequest *request,
                        ::txlog::TransferResponse *response,
                        ::google::protobuf::Closure *done) override
    {
    }

    void CheckHealth(::google::protobuf::RpcController *controller,
                     const ::txlog::HealthzHttpRequest *request,
                     ::txlog::HealthzHttpResponse *response,
                     ::google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller *cntl = static_cast<brpc::Controller *>(controller);

        // Populate the response
        txlog::RaftStat *raft_stat = response->add_raft_stat();
        raft_stat->set_log_group("0");
        raft_stat->set_state("LEADER");

        // Serialize to JSON
        std::string json_str;
        if (json2pb::ProtoMessageToJson(*response, &json_str))
        {
            // Attach the JSON string to the response
            cntl->response_attachment().append(json_str);
            cntl->http_response().set_content_type("application/json");
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_OK);
        }
        else
        {
            // Handle serialization failure
            cntl->response_attachment().append(
                "Failed to serialize response to JSON");
            cntl->http_response().set_status_code(
                brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        }
    }

    void RemoveCcNodeGroup(::google::protobuf::RpcController *controller,
                           const LogRequest *request,
                           LogResponse *response,
                           ::google::protobuf::Closure *done) override
    {
    }

    void AddPeer(::google::protobuf::RpcController *controller,
                 const AddPeerRequest *request,
                 ChangePeersResponse *response,
                 google::protobuf::Closure *done) override
    {
        LOG(WARNING) << "AddPeer not implemented";
        assert(false);
    }

    void RemovePeer(::google::protobuf::RpcController *controller,
                    const RemovePeerRequest *request,
                    ChangePeersResponse *response,
                    google::protobuf::Closure *done) override
    {
        LOG(WARNING) << "RemovePeer not implemented";
        assert(false);
    }

    void GetLogGroupConfig(::google::protobuf::RpcController *controller,
                           const GetLogGroupConfigRequest *request,
                           GetLogGroupConfigResponse *response,
                           ::google::protobuf::Closure *done) override
    {
        LOG(WARNING) << "GetLogGroupConfig not implemented";
        assert(false);
    }

    int Start(LogState *log_state, uint32_t worker_concurrency = 4);
    void Shutdown();

    static void SetWriteLogErrorResponse(const WriteLogRequest &request,
                                         LogResponse *response);

private:
    void SubmitAndWait(const LogRequest *req, LogResponse *resp);

    const uint32_t log_group_id_;
    std::vector<std::unique_ptr<OpenLogTaskWorker>> workers_;
    // received task counter per each LogService thread
    static thread_local size_t received_task_cnt_;

    LogState *log_state_{nullptr};
    std::mutex log_replay_workers_mutex_;
    std::unique_ptr<LogShippingAgent> log_replay_worker_;
};
}  // namespace txlog