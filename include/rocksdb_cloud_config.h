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

#if defined(USE_ROCKSDB_LOG_STATE) && defined(WITH_ROCKSDB_CLOUD)
#include <string>

#include "rocksdb/cloud/db_cloud.h"

#include "log_utils.h"

namespace txlog
{
struct RocksDBCloudConfig
{
    RocksDBCloudConfig() = default;

    RocksDBCloudConfig(std::string &aws_access_key_id,
                       std::string &aws_secret_key,
                       std::string &bucket_name,
                       std::string &bucket_prefix,
                       std::string &region,
                       uint64_t sst_file_cache_size,
                       uint32_t db_ready_timeout,
                       uint32_t db_file_deletion_delay,
                       uint32_t log_retention_days,
                       uint32_t log_purger_starting_hour,
                       uint32_t log_purger_starting_minute,
                       uint32_t log_purger_starting_second)
        : aws_access_key_id_(aws_access_key_id),
          aws_secret_key_(aws_secret_key),
          bucket_name_(bucket_name),
          bucket_prefix_(bucket_prefix),
          region_(region),
          sst_file_cache_size_(sst_file_cache_size),
          db_ready_timeout_us_(db_ready_timeout * 1000 * 1000),
          db_file_deletion_delay_(db_file_deletion_delay),
          log_retention_days_(log_retention_days),
          log_purger_starting_hour_(log_purger_starting_hour),
          log_purger_starting_minute_(log_purger_starting_minute),
          log_purger_starting_second_(log_purger_starting_second)
    {
    }

    std::string aws_access_key_id_;
    std::string aws_secret_key_;
    std::string bucket_name_;
    std::string bucket_prefix_;
    std::string region_;
    uint64_t sst_file_cache_size_;
    uint32_t db_ready_timeout_us_;
    uint32_t db_file_deletion_delay_;
    uint32_t log_retention_days_;
    uint32_t log_purger_starting_hour_;
    uint32_t log_purger_starting_minute_;
    uint32_t log_purger_starting_second_;
};

inline rocksdb::Status NewCloudFileSystem(
    const rocksdb::CloudFileSystemOptions &cfs_options,
    rocksdb::CloudFileSystem **cfs)
{
#if (WITH_ROCKSDB_CLOUD == CS_TYPE_S3)
    // AWS s3 file system
    auto status = rocksdb::CloudFileSystemEnv::NewAwsFileSystem(
        rocksdb::FileSystem::Default(),
        cfs_options,
        nullptr,
        cfs);
#elif (WITH_ROCKSDB_CLOUD == CS_TYPE_GCS)
    // Google cloud storage file system
    auto status = rocksdb::CloudFileSystem::NewGcpFileSystem(
        rocksdb::FileSystem::Default(),
        cfs_options,
        nullptr,
        cfs);
#endif
    return status;
};

}// namespace txlog
#endif