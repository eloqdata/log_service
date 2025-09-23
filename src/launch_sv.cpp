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
#include <gflags/gflags_declare.h>
#include <unistd.h>

#include <cassert>
#include <cstdint>

#include "INIReader.h"
#include "log_server.h"
#include "log_utils.h"

#if BRPC_WITH_GLOG
#include "glog_error_logging.h"
#endif

#ifdef OVERRIDE_GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = gflags;
#else
#ifndef GFLAGS_NAMESPACE
namespace GFLAGS_NAMESPACE = google;
#endif
#endif

DEFINE_string(config_path, "", "Configuration file path");

DEFINE_uint32(start_log_group_id, 0, "Start log group id");
DEFINE_string(conf, "", "Initial configuration of all group");
DEFINE_int32(node_id, 0, "node id in conf");
DEFINE_uint32(snapshot_interval, 600, "snapshot interval");
DEFINE_string(storage_path, "/tmp/log_service/raft_data", "raft storage path");
DEFINE_uint32(in_mem_data_log_queue_size_high_watermark,
              50 * 10000,
              "In memory data log queue max size");
DEFINE_uint32(log_group_replica_num, 3, "replica number in one log group");
DEFINE_uint32(rocksdb_max_write_buffer_number, 8, "Max write buffer number");
DEFINE_uint32(rocksdb_max_background_jobs, 12, "Max background jobs");
DEFINE_string(rocksdb_target_file_size_base,
              "64MB",
              "Target file size base for rocksdb");
DEFINE_string(rocksdb_sst_files_size_limit,
              "500MB",
              "The total RocksDB sst files size before purge");

DEFINE_bool(enable_request_checkpoint,
            false,
            "Enable sending checkpoint requests when the criteria are met.");

DEFINE_uint32(check_replay_log_size_interval_sec,
              10,
              "The interval for checking txlogs size used in tx recovery.");

DEFINE_string(notify_checkpointer_threshold_size,
              "1GB",
              "When the size of non-checkpoint txlogs reache this threshold, "
              "the log_service sends a checkpoint request to tx_service.");

DEFINE_bool(enable_brpc_builtin_services,
            true,
            "Enable showing brpc builtin services through http.");

DEFINE_uint32(rocksdb_scan_threads, 1, "The number of rocksdb scan threads");

DEFINE_string(log_file_name_prefix,
              "log-service.log",
              "Sets the prefix for log files. Default is 'log-service.log'");

static bool CheckCommandLineFlagIsDefault(const char *name)
{
    gflags::CommandLineFlagInfo flag_info;

    bool flag_found = gflags::GetCommandLineFlagInfo(name, &flag_info);
    // Make sure the flag is declared.
    assert(flag_found);
    (void) flag_found;

    // Return `true` if the flag has the default value and has not been set
    // explicitly from the cmdline or via SetCommandLineOption
    return flag_info.is_default;
}

void launch(const std::string &tt_conf,
            uint32_t node_id,
            const std::string &storage_path
)
{
    std::vector<std::string> ip_list;
    std::vector<uint16_t> port_list;
    std::vector<std::string> ip_port_list = txlog::split(tt_conf, ",");
    if (node_id >= ip_port_list.size())
    {
        LOG(ERROR) << "Invalid configuration: `node_id` must be less than node "
                      "size, node id = "
                   << node_id << ", node size = " << ip_port_list.size();
        if (!FLAGS_alsologtostderr)
        {
            std::cout
                << "Failed to start LogServer, invalid configuration of node_id"
                << std::endl;
        }
        return;
    }
    for (const auto &ip_port : ip_port_list)
    {
        auto p = ip_port.find(':');
        if (p == std::string::npos)
        {
            LOG(ERROR) << "Invalid configuration: expecting "
                          "ip:port,ip:port,ip:port... in conf";
            if (!FLAGS_alsologtostderr)
            {
                std::cout << "Failed to start LogServer, invalid configuration "
                             ": conf"
                          << std::endl;
            }
            return;
        }
        std::string ip = ip_port.substr(0, p), port = ip_port.substr(p + 1);
        ip_list.push_back(ip);
        port_list.emplace_back(std::stoi(port));
    }

    uint64_t notify_checkpointer_threshold_size =
        txlog::parse_size(FLAGS_notify_checkpointer_threshold_size);
    uint64_t rocksdb_target_file_size_base =
        txlog::parse_size(FLAGS_rocksdb_target_file_size_base);
    size_t rocksdb_sst_files_size_limit =
        txlog::parse_size(FLAGS_rocksdb_sst_files_size_limit);
    txlog::LogServer server(node_id,
                            port_list[node_id],
                            storage_path,
                            FLAGS_rocksdb_scan_threads,
                            rocksdb_sst_files_size_limit,
                            FLAGS_rocksdb_max_write_buffer_number,
                            FLAGS_rocksdb_max_background_jobs,
                            rocksdb_target_file_size_base);

    int start_status = server.Start(FLAGS_enable_brpc_builtin_services);
    if (start_status != 0)
    {
        LOG(ERROR) << "Failed to start log server, status: " << start_status;
        if (!FLAGS_alsologtostderr)
        {
            std::cout << "Failed to start LogServer, please check log file for "
                         "details."
                      << std::endl;
        }
        return;
    }
    if (!FLAGS_alsologtostderr)
    {
        std::cout << "LogServer Started, listenning on port "
                  << port_list[node_id] << "." << std::endl;
    }

    while (true)
    {
        sleep(100);
    }
    return;
}

void PrintHelloText()
{
    std::cout << "* Welcome to use LogServer." << std::endl;
    std::cout << "* Running logs will be written to the following path:"
              << std::endl;
    std::cout << FLAGS_log_dir << std::endl;
    std::cout << "* The above log path can be specified by arg --log_dir."
              << std::endl;
    std::cout << "* You can also run with [--help] for all available flags."
              << std::endl;
    std::cout << std::endl;
}

int main(int argc, char *argv[])
{
    // Increase max allowed rpc message size to 512mb.
    GFLAGS_NAMESPACE::SetCommandLineOption("max_body_size", "536870912");
    GFLAGS_NAMESPACE::SetCommandLineOption("graceful_quit_on_sigterm", "true");
    GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_GLOG
    InitGoogleLogging(argv);
#endif

    std::string config_file = FLAGS_config_path;
    INIReader config_reader(config_file);
    if (!config_file.empty() && config_reader.ParseError() != 0)
    {
        LOG(ERROR) << "Failed to parse config file, The first error line is "
                   << config_reader.ParseError();

        if (!FLAGS_alsologtostderr)
        {
            std::cout << "Failed to parse config file, The first error line is "
                      << config_reader.ParseError();
        }
        return -1;
    }

    FLAGS_log_file_name_prefix =
        !CheckCommandLineFlagIsDefault("log_file_name_prefix")
            ? FLAGS_log_file_name_prefix
            : config_reader.GetString(
                  "local", "log_file_name_prefix", FLAGS_log_file_name_prefix);

    FLAGS_start_log_group_id =
        !CheckCommandLineFlagIsDefault("start_log_group_id")
            ? FLAGS_start_log_group_id
            : config_reader.GetInteger(
                  "local", "start_log_group_id", FLAGS_start_log_group_id);
    FLAGS_conf = !CheckCommandLineFlagIsDefault("conf")
                     ? FLAGS_conf
                     : config_reader.GetString("local", "conf", FLAGS_conf);
    FLAGS_node_id =
        !CheckCommandLineFlagIsDefault("node_id")
            ? FLAGS_node_id
            : config_reader.GetInteger("local", "node_id", FLAGS_node_id);
    FLAGS_snapshot_interval =
        !CheckCommandLineFlagIsDefault("snapshot_interval")
            ? FLAGS_snapshot_interval
            : config_reader.GetInteger(
                  "local", "snapshot_interval", FLAGS_snapshot_interval);
    FLAGS_storage_path = !CheckCommandLineFlagIsDefault("storage_path")
                             ? FLAGS_storage_path
                             : config_reader.GetString(
                                   "local", "storage_path", FLAGS_storage_path);
    FLAGS_in_mem_data_log_queue_size_high_watermark =
        !CheckCommandLineFlagIsDefault(
            "in_mem_data_log_queue_size_high_watermark")
            ? FLAGS_in_mem_data_log_queue_size_high_watermark
            : config_reader.GetInteger(
                  "rocksdb",
                  "in_mem_data_log_queue_size_high_watermark",
                  FLAGS_in_mem_data_log_queue_size_high_watermark);
    FLAGS_log_group_replica_num =
        !CheckCommandLineFlagIsDefault("log_group_replica_num")
            ? FLAGS_log_group_replica_num
            : config_reader.GetInteger("local",
                                       "log_group_replica_num",
                                       FLAGS_log_group_replica_num);
    FLAGS_rocksdb_max_write_buffer_number =
        !CheckCommandLineFlagIsDefault("rocksdb_max_write_buffer_number")
            ? FLAGS_rocksdb_max_write_buffer_number
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_max_write_buffer_number",
                                       FLAGS_rocksdb_max_write_buffer_number);
    FLAGS_rocksdb_max_background_jobs =
        !CheckCommandLineFlagIsDefault("rocksdb_max_background_jobs")
            ? FLAGS_rocksdb_max_background_jobs
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_max_background_jobs",
                                       FLAGS_rocksdb_max_background_jobs);
    FLAGS_rocksdb_target_file_size_base =
        !CheckCommandLineFlagIsDefault("rocksdb_target_file_size_base")
            ? FLAGS_rocksdb_target_file_size_base
            : config_reader.GetString("rocksdb",
                                      "rocksdb_target_file_size_base",
                                      FLAGS_rocksdb_target_file_size_base);
    FLAGS_rocksdb_sst_files_size_limit =
        !CheckCommandLineFlagIsDefault("rocksdb_sst_files_size_limit")
            ? FLAGS_rocksdb_sst_files_size_limit
            : config_reader.GetString("rocksdb",
                                      "rocksdb_sst_files_size_limit",
                                      FLAGS_rocksdb_sst_files_size_limit);
    FLAGS_enable_request_checkpoint =
        !CheckCommandLineFlagIsDefault("enable_request_checkpoint")
            ? FLAGS_enable_request_checkpoint
            : config_reader.GetBoolean("local",
                                       "enable_request_checkpoint",
                                       FLAGS_enable_request_checkpoint);
    FLAGS_check_replay_log_size_interval_sec =
        !CheckCommandLineFlagIsDefault("check_replay_log_size_interval_sec")
            ? FLAGS_check_replay_log_size_interval_sec
            : config_reader.GetInteger(
                  "local",
                  "check_replay_log_size_interval_sec",
                  FLAGS_check_replay_log_size_interval_sec);
    FLAGS_notify_checkpointer_threshold_size =
        !CheckCommandLineFlagIsDefault("notify_checkpointer_threshold_size")
            ? FLAGS_notify_checkpointer_threshold_size
            : config_reader.GetString("local",
                                      "notify_checkpointer_threshold_size",
                                      FLAGS_notify_checkpointer_threshold_size);
    FLAGS_enable_brpc_builtin_services =
        !CheckCommandLineFlagIsDefault("enable_brpc_builtin_services")
            ? FLAGS_enable_brpc_builtin_services
            : config_reader.GetBoolean("local",
                                       "enable_brpc_builtin_services",
                                       FLAGS_enable_brpc_builtin_services);
    FLAGS_rocksdb_scan_threads =
        !CheckCommandLineFlagIsDefault("rocksdb_scan_threads")
            ? FLAGS_rocksdb_scan_threads
            : config_reader.GetInteger("rocksdb",
                                       "rocksdb_scan_threads",
                                       FLAGS_rocksdb_scan_threads);

    if (!FLAGS_alsologtostderr)
    {
        PrintHelloText();
        std::cout << "Starting log server with follow configs ..."
                  << "\n conf: " << FLAGS_conf << "; "
                  << "start log group id: " << FLAGS_start_log_group_id << "; "
                  << "node_id: " << FLAGS_node_id << "; "
                  << "raft storage_path: " << FLAGS_storage_path << "; "
                  << std::endl;
    }
    LOG(INFO) << "log server starting... conf: " << FLAGS_conf << "; "
              << "start log group id: " << FLAGS_start_log_group_id << "; "
              << "node_id: " << FLAGS_node_id << "; "
              << "raft storage_path: " << FLAGS_storage_path << "; "
              << std::endl;

    launch(FLAGS_conf,
           FLAGS_node_id,
           "local://" + FLAGS_storage_path
    );

    if (!FLAGS_alsologtostderr)
    {
        std::cout << "LogServer Stopped." << std::endl;
    }

    return 0;
}
