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
#include <gflags/gflags.h>
#include <unistd.h>

#include "log_agent.h"
#include "raft_server.h"

DEFINE_string(ip, "10.3.1.17", "ip address");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(id, 0, "raft instance id");
DEFINE_string(raft_conf,
              "10.3.1.17:8100:0",
              "Initial configuration of the replication group");
DEFINE_string(group, "0", "which group this raft instance is in");
DEFINE_string(storage_path, "local:///mnt/raft_data", "raft storage path");

void run_direct(std::string ip,
                uint32_t port,
                uint32_t id,
                std::string raft_conf,
                std::string group,
                std::string storage_path)
{
    txlog::RaftServer raftServer(FLAGS_ip,
                                 FLAGS_port,
                                 FLAGS_id,
                                 FLAGS_raft_conf,
                                 FLAGS_group,
                                 FLAGS_storage_path);
    raftServer.Run();
}

void run_service(std::string ip,
                 uint32_t port,
                 uint32_t id,
                 std::string raft_conf,
                 std::string group,
                 std::string storage_path)
{
    std::vector<std::string> ips = {ip};
    std::vector<uint32_t> ports = {port};
    std::vector<uint32_t> ids = {id};
    std::vector<std::string> raft_confs = {raft_conf};
    std::vector<std::string> groups = {group};
    std::vector<std::string> storage_paths = {storage_path};
    txlog::LogAgent log_agent(
        raft_conf, ips, ports, ids, raft_confs, groups, storage_paths);
    log_agent.StartService();
    while (true)
    {
        sleep(100);
    }
}

int main(int argc, char *argv[])
{
    google::ParseCommandLineFlags(&argc, &argv, true);
    run_service(FLAGS_ip,
                FLAGS_port,
                FLAGS_id,
                FLAGS_raft_conf,
                FLAGS_group,
                FLAGS_storage_path);

    return 0;
}
