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
#include "log_server.h"

#include <gflags/gflags.h>
#include <unistd.h>

#include <cstdint>

#include "log_state_rocksdb_impl.h"
#include "log_utils.h"

// gflags 2.1.1 missing GFLAGS_NAMESPACE. This is a workaround to handle gflags
// ABI issue.
#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#else
#ifndef GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = google;
#endif
#endif

namespace bthread
{
DECLARE_int32(bthread_concurrency);
}

namespace txlog
{
/*
 * LogServer is the driver of log service.
 *
 * Parameter:
 * node_id: the current log node id.
 * port: port of current log node, which is used to start log group rpc service.
 * ip_list: ip list of all the log node in the cluster.
 * port_list: port list of all the log node in the cluster.
 * storage_path: the location to store the raft file.
 *
 * TODO: The architecture of log service is decoupled with other components
 * including runtime, txservice (cc node) and storage engine. Hence log service
 * could have its own configuration in future.
 */
LogServer::LogServer(uint32_t node_id,
                     uint16_t port,
                     const std::string &storage_path,
                     const size_t rocksdb_scan_threads,
#ifdef WITH_ROCKSDB_CLOUD
                     RocksDBCloudConfig rocksdb_cloud_config,
                     const size_t in_mem_data_log_queue_size_high_watermark,
#else
                     const size_t sst_files_size_limit,
#endif
                     const size_t rocksdb_max_write_buffer_number,
                     const size_t rocksdb_max_background_jobs,
                     const size_t rocksdb_target_file_size_base)
    : open_log_service_(node_id), port_(port)
{
#ifdef WITH_ROCKSDB_CLOUD
    log_state_ = std::make_unique<LogStateRocksDBCloudImpl>(
        storage_path.substr(8) + "/rocksdb",
        rocksdb_cloud_config,
        this,
        in_mem_data_log_queue_size_high_watermark,
        rocksdb_max_write_buffer_number,
        rocksdb_max_background_jobs,
        rocksdb_target_file_size_base,
        rocksdb_scan_threads);
#else
    // specify log_state rocksdb path from braft storage path, trimming
    // "local://" prefix.
    log_state_ = std::make_unique<LogStateRocksDBImpl>(
        storage_path.substr(8) + "/rocksdb",
        sst_files_size_limit,
        rocksdb_scan_threads);
#endif
}

int LogServer::Start(bool enable_brpc_builtin_services)
{
    SetCommandLineOptions();

    // Add log service into RPC server. SERVER_DOESNT_OWN_SERVICE means
    // brpc_server_ doesn't manage the lifecycle of log_service_. They are both
    // handled by LogServer.
    // Add your service into RPC server
    // and specify a URL for `CheckHealth` method in the service
    if (brpc_server_.AddService(&open_log_service_,
                                brpc::SERVER_DOESNT_OWN_SERVICE,
                                "/healthz => CheckHealth") != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    if (open_log_service_.Start(log_state_.get()) != 0)
    {
        LOG(ERROR) << "Fail to start open log service";
        return -1;
    }

    brpc::ServerOptions options;
    // use all cpu cores if flag bthread_concurrency is not set
    options.num_threads = bthread::FLAGS_bthread_concurrency;
    options.has_builtin_services = enable_brpc_builtin_services;
    if (IsDefaultGFlag("bthread_concurrency") && NUM_VCPU)
        options.num_threads = NUM_VCPU + 1;

    if (brpc_server_.Start(port_, &options) != 0)
    {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    LOG(INFO) << "Raft Service is running on " << brpc_server_.listen_address();

    return 0;
}

void LogServer::SetCommandLineOptions()
{
    // enable raft leader lease
    GFLAGS_NAMESPACE::SetCommandLineOption("raft_enable_leader_lease", "true");

    // set brpc circuit_breaker max isolation duration smaller than election
    // timeout so that restarted node will join raft group before trying to
    // start a new vote
    GFLAGS_NAMESPACE::SetCommandLineOption(
        "circuit_breaker_max_isolation_duration_ms", "4500");
}
}  // namespace txlog
