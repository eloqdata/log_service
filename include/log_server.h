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

#include <brpc/server.h>

#include <cstdint>
#include <string>

#include "open_log_service.h"

namespace txlog
{
const auto NUM_VCPU = std::thread::hardware_concurrency();

/*
 * LogServer is the driver of log service. It used to
 * 1. setup route table for braft service.
 * 2. initialize raft log service.
 * 3. register brpc server with raft log service and braft service.
 * 4. start brpc server and braft state machine.
 */
class LogServer
{
public:
    LogServer(uint32_t node_id,
              uint16_t port,
              const std::string &storage_path,
              const size_t rocksdb_scan_threads,
              const size_t sst_files_size_limit = 500 * 1024 * 1024,
              const size_t rocksdb_max_write_buffer_number = 8,
              const size_t rocksdb_max_background_jobs = 12,
              const size_t rocksdb_target_file_size_base = 64 * 1024 * 1024);

    ~LogServer()
    {
        brpc_server_.Stop(0);  // legacy parameter. Just pass in 0.
        brpc_server_.Join();

        open_log_service_.Shutdown();
        // open_log_service_.Join();
    }

    void Close()
    {
        open_log_service_.Shutdown();
        brpc_server_.Stop(0);
    }

    int Start(bool enable_brpc_builtin_services = true);

private:
    brpc::Server brpc_server_;
    OpenLogServiceImpl open_log_service_;

    uint32_t port_;
    std::unique_ptr<LogState> log_state_;

    void SetCommandLineOptions();
};
}  // namespace txlog
