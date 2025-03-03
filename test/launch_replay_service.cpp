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
#include <brpc/server.h>

#include <iostream>

#include "replay_service_for_test.h"

DEFINE_int32(replay_port, 8888, "Port of the replay service");

int start_replay_rpc_server()
{
    brpc::Server server;
    ReplayService replay_service_impl;
    // add replay service into server
    if (server.AddService(&replay_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
    {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // start server
    brpc::ServerOptions options;
    if (server.Start(FLAGS_replay_port, &options) != 0)
    {
        LOG(ERROR) << "Fail to start ReplayServer";
        return -1;
    }
    LOG(INFO) << "log replay server started";
    server.RunUntilAskedToQuit();
    return 0;
}

int main(int argc, char *argv[])
{
    start_replay_rpc_server();
}
