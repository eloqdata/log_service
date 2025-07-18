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

#include <butil/logging.h>

#include <array>
#include <cstdint>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "log.pb.h"

namespace txlog
{
enum struct LogItemType
{
    DataLog,
    SchemaLog,
    SplitRangeLog,
    ClusterScaleLog
};

struct Item
{
public:
    using Pointer = std::shared_ptr<Item>;

    Item() = default;

    Item(uint64_t tx_number,
         uint64_t timestamp,
         std::string log_message,
         LogItemType item_type)
        : tx_number_(tx_number),
          timestamp_(timestamp),
          log_message_(std::move(log_message)),
          item_type_(item_type)
    {
    }

    uint64_t tx_number_;
    uint64_t timestamp_;
    std::string log_message_;
    LogItemType item_type_;
};

class ItemIterator
{
public:
    explicit ItemIterator(std::vector<Item::Pointer> &&item_list)
        : ddl_list_(std::move(item_list)), ddl_idx_(0) {};
    virtual ~ItemIterator() = default;
    ItemIterator(const ItemIterator &) = delete;
    void operator=(const ItemIterator &) = delete;

    virtual void SeekToFirst() = 0;
    virtual bool Valid() = 0;
    virtual void Next() = 0;
    virtual const Item &GetItem() = 0;

    virtual void SeekToDDLFirst() = 0;
    virtual bool ValidDDL() = 0;
    virtual void NextDDL() = 0;
    virtual const Item &GetDDLItem() = 0;

    virtual size_t IteratorNum() = 0;
    virtual void SeekToFirst(size_t idx) = 0;
    virtual bool Valid(size_t idx) = 0;
    virtual void Next(size_t idx) = 0;
    virtual const Item &GetItem(size_t idx) = 0;

protected:
    std::vector<Item::Pointer> ddl_list_;
    size_t ddl_idx_{0};
};

class LogState
{
public:
    using Pointer = std::unique_ptr<LogState>;

    enum struct MetaOp : uint8_t
    {
        SchemaOp = 0,
        RangeOp,
        LastCkpt,
        MaxTxn
    };

    LogState() = default;
    virtual ~LogState() = default;

    virtual int AddLogItem(uint64_t tx_number,
                           uint64_t timestamp,
                           const std::string &log_message) = 0;

    virtual int AddLogItemBatch(
        const std::vector<std::tuple<uint64_t, uint64_t, std::string>>
            &batch_logs)
    {
        int err = 0;
        for (const auto &[tx, ts, log_message] : batch_logs)
        {
            err = AddLogItem(tx, ts, log_message);
            if (err != 0)
                break;
        }
        return err;
    }

    virtual std::pair<bool, std::unique_ptr<ItemIterator>> GetLogReplayList(
        uint64_t start_timestamp) = 0;

    virtual std::pair<bool, Item::Pointer> SearchTxDataLog(
        uint64_t tx_number, uint64_t lower_bound_ts = 0) = 0;

    /**
     * Stores cc node's latest state.
     *
     * latest_txn_no_ is to keep track of the cc node's latest committed txn
     * number, so we don't get repeated txn numbers.
     *
     * last_ckpt_ts_ stores this cc node's last checkpoint timestamp on this log
     * group, for ReplayLog usage.
     * Besides, for LogStateRocksDBImpl, since the oldest items are deleted,
     * there can be a large amount of "tombstones" in the beginning of each
     * ng_id. As a result, this query might be exceptionally slow: Seek(<0, 0>).
     * To mitigate this problem, remember the last truncate timestamp and
     * iterate from <last_ckpt_ts, 0>.
     */
    struct CcNgInfo
    {
        CcNgInfo() = default;

        explicit CcNgInfo(std::string leader_ip,
                          uint32_t leader_port,
                          uint32_t latest_txn_no = 0,
                          uint64_t ckpt_ts = 0)
            : leader_ip_(leader_ip),
              leader_port_(leader_port),
              latest_txn_no_(latest_txn_no),
              last_ckpt_ts_(ckpt_ts)
        {
        }

        CcNgInfo &operator=(const CcNgInfo &rhs)
        {
            if (this == &rhs)
            {
                return *this;
            }

            leader_ip_ = rhs.leader_ip_;
            leader_port_ = rhs.leader_port_;
            latest_txn_no_.store(
                rhs.latest_txn_no_.load(std::memory_order_relaxed));
            last_ckpt_ts_.store(
                rhs.last_ckpt_ts_.load(std::memory_order_relaxed));

            return *this;
        }

        std::string leader_ip_;
        uint32_t leader_port_{};
        std::atomic<uint32_t> latest_txn_no_{};
        std::atomic<uint64_t> last_ckpt_ts_{};
    };

    /**
     * Search schema log of transaction tx_number
     * @param tx_number
     * @return whether the schema log is committed and the stage
     */
    std::pair<bool, SchemaOpMessage_Stage> SearchTxSchemaLog(uint64_t tx_number)
    {
        // this func is called in RecoverTx rpc thread, might be concurrent with
        // braft on_apply when processing WriteLogRequest
        std::shared_lock s_lk(log_state_mutex_);

        auto catalog_it = tx_catalog_ops_.find(tx_number);
        if (catalog_it == tx_catalog_ops_.end())
        {
            return {false, SchemaOpMessage_Stage_Stage_MIN};
        }
        return {true, catalog_it->second.SchemasOpStage()};
    }

    virtual int Start()
    {
        return 0;
    }

    bool UpsertSchemaOp(uint64_t tx_no,
                        uint64_t commit_ts,
                        const SchemaOpMessage &schema_op)
    {
        const SchemaOpMessage::Stage new_stage = schema_op.stage();
        if (new_stage == SchemaOpMessage_Stage_PrepareSchema)
        {
            // only insert new entry at prepare stage
            auto [it, success] =
                tx_catalog_ops_.try_emplace(tx_no, schema_op, commit_ts);
            if (!success)
            {
                LOG(INFO) << "duplicate prepare log detected, txn: " << tx_no
                          << ", ignore";
                return true;
            }
        }
        if (PersistSchemaOp(tx_no, commit_ts, schema_op) != 0)
        {
            return false;
        }

        std::unique_lock lk(log_state_mutex_);
        assert(!schema_op.table_name_str().empty() &&
               schema_op.table_type() == CcTableType::Primary);

        if (new_stage != SchemaOpMessage_Stage_PrepareSchema)
        {
            auto catalog_it = tx_catalog_ops_.find(tx_no);
            if (catalog_it == tx_catalog_ops_.end())
            {
                return true;
            }

            // The schema operation has been logged. Only updates the stage.
            CatalogOp &catalog_op = catalog_it->second;
            if (new_stage > catalog_op.SchemasOpStage())
            {
                // For the schema operation that need to deal with the data,
                // such as ADD INDEX.
                if (new_stage == SchemaOpMessage_Stage_PrepareData)
                {
                    SchemaOpMessage &schema_op_msg = *catalog_op.GetSchemaOpMsg(
                        schema_op.table_name_str(), schema_op.table_type());
                    schema_op_msg.set_last_key_type(schema_op.last_key_type());
                    schema_op_msg.set_last_key_value(
                        schema_op.last_key_value());
                    schema_op_msg.set_new_catalog_blob(
                        schema_op.new_catalog_blob());
                }

                // Encounter flush error after write prepare log. Need to
                // set commit_ts_ to 0(previously set by prepare_log)
                if (commit_ts == 0 &&
                    new_stage == SchemaOpMessage_Stage_CommitSchema)
                {
                    catalog_op.SetCommitTs(0);
                }

                // Schema logs at CleanSchema stage will be kept in LogState
                // for some time instead of be erased immediately. This is
                // to filter those retried stale WriteLogRequest of previous
                // stage (prepare log) yet come to log service after clean
                // log finished. The schema log will be erased when all node
                // group's ckpt_ts are one hour greater than its commit ts.
                if (new_stage == SchemaOpMessage_Stage_CleanSchema)
                {
                    // For pure DDL transactions, CatalogOp contains only
                    // one catalog image. For logical alter table DDL inside
                    // DML transactions, CatalogOp might contain multiple
                    // catalog images. Given one catalog, recovery for the
                    // above two scene share same DDL recovery workflow.
                    // Clear the specified SchemaOpMsg.
                    catalog_op.Clear(schema_op.table_name_str(),
                                     schema_op.table_type());
                }
                else
                {
                    catalog_op
                        .GetSchemaOpMsg(schema_op.table_name_str(),
                                        schema_op.table_type())
                        ->set_stage(new_stage);
                }
            }
            else if (new_stage == catalog_op.SchemasOpStage())
            {
                if (new_stage == SchemaOpMessage_Stage_PrepareData)
                {
                    SchemaOpMessage &schema_op_msg = *catalog_op.GetSchemaOpMsg(
                        schema_op.table_name_str(), schema_op.table_type());
                    schema_op_msg.set_last_key_type(schema_op.last_key_type());
                    schema_op_msg.set_last_key_value(
                        schema_op.last_key_value());
                    schema_op_msg.set_new_catalog_blob(
                        schema_op.new_catalog_blob());
                }
            }
            else
            {
                return true;
            }
        }
        return true;
    }

    bool UpsertSchemaOpWithinDML(
        uint64_t tx_no,
        uint64_t commit_ts,
        const ::google::protobuf::RepeatedPtrField<SchemaOpMessage> &schemas_op)
    {
        int rc = PersistSchemasOp(tx_no, commit_ts, schemas_op);
        if (rc != 0)
        {
            return false;
        }

        // this func is called when on_apply processing WriteLogRequest, might
        // be concurrent with SearchTxSchemaLog() in RecoverTx rpc thread
        std::unique_lock lk(log_state_mutex_);

        assert(commit_ts > 0);

        SchemaOpMessage::Stage new_stage = schemas_op.at(0).stage();
        auto [it, success] =
            tx_catalog_ops_.try_emplace(tx_no, schemas_op, commit_ts);
        if (!success)
        {
            CatalogOp &catalog_op = it->second;
            SchemaOpMessage::Stage old_stage = catalog_op.SchemasOpStage();
            if (new_stage > old_stage)
            {
                if (new_stage == SchemaOpMessage_Stage_CommitSchema &&
                    old_stage == SchemaOpMessage_Stage_PrepareSchema)
                {
                    catalog_op.CommitAll();
                }
                else if (new_stage == SchemaOpMessage_Stage_CleanSchema &&
                         old_stage == SchemaOpMessage_Stage_CommitSchema)
                {
                    catalog_op.ClearAll();
                }
            }
            else

            {
                LOG(INFO) << "duplicate commit log detected, txn: " << tx_no
                          << ", ignore";
            }
        }
        return true;
    }

    std::pair<bool, SplitRangeOpMessage_Stage> SearchTxSplitRangeOp(
        uint64_t tx_number)
    {
        // this func is called in RecoverTx rpc thread, might be concurrent
        // with braft on_apply when processing WriteLogRequest
        std::shared_lock s_lk(log_state_mutex_);
        auto iter = tx_split_range_ops_.find(tx_number);
        if (iter == tx_split_range_ops_.end())
        {
            return {false, SplitRangeOpMessage_Stage_Stage_MIN};
        }
        else
        {
            return {true, iter->second.split_range_op_message_.stage()};
        }
    }

    void UpdateSplitRangeOp(uint64_t tx_num,
                            uint64_t commit_ts,
                            const SplitRangeOpMessage &split_range_op_message)
    {
        int rc = PersistRangeOp(tx_num, commit_ts, split_range_op_message);
        while (rc != 0)
        {
            rc = PersistRangeOp(tx_num, commit_ts, split_range_op_message);
        }

        std::unique_lock x_lk(log_state_mutex_);

        SplitRangeOpMessage::Stage new_stage = split_range_op_message.stage();
        // only insert new entry at prepare stage
        if (new_stage == SplitRangeOpMessage_Stage_PrepareSplit)
        {
            auto [it, success] = tx_split_range_ops_.try_emplace(
                tx_num, split_range_op_message, commit_ts);
            if (!success)
            {
                LOG(INFO) << "duplicate split range prepare log detected, txn: "
                          << tx_num << ", ignore";
                return;
            }
        }
        else
        {
            auto split_range_op_it = tx_split_range_ops_.find(tx_num);

            if (split_range_op_it == tx_split_range_ops_.end())
            {
                return;
            }

            SplitRangeOpMessage &split_range_msg =
                split_range_op_it->second.split_range_op_message_;
            if (new_stage > split_range_msg.stage())
            {
                if (new_stage == SplitRangeOpMessage_Stage_CommitSplit)
                {
                    // slice specs are just written in commit stage log.
                    split_range_msg.clear_slice_keys();
                    split_range_msg.clear_slice_sizes();

                    assert(split_range_op_message.slice_keys_size() + 1 ==
                           split_range_op_message.slice_sizes_size());
                    int idx = 0;
                    for (; idx < split_range_op_message.slice_keys_size();
                         idx++)
                    {
                        split_range_msg.add_slice_keys(
                            split_range_op_message.slice_keys(idx));
                        split_range_msg.add_slice_sizes(
                            split_range_op_message.slice_sizes(idx));
                    }
                    split_range_msg.add_slice_sizes(
                        split_range_op_message.slice_sizes(idx));
                }
                else
                {
                    // SplitRange logs at CleanSplit stage will be kept in
                    // LogState for some time instead of be erased
                    // immediately. This is to filter those retried stale
                    // WriteLogRequest of previous stage (prepare log) yet
                    // come to log service after clean log finished. The
                    // SplitRange log will be erased when all node group's
                    // ckpt_ts are one hour greater than its commit ts.
                    assert(new_stage ==
                           SplitRangeOpMessage_Stage::
                               SplitRangeOpMessage_Stage_CleanSplit);

                    // Free the memory used by split_range_msg by overriding
                    // it as split_range_msg.Clear() won't free the memory
                    // used by message.
                    split_range_op_it->second.split_range_op_message_ =
                        SplitRangeOpMessage();
                }
                split_range_msg.set_stage(new_stage);
            }
            else
            {
                LOG(INFO) << "duplicate split range log detected, txn: "
                          << tx_num << ", stage: " << int(new_stage)
                          << ", ignore";
                return;
            }
        }
    }

    void CleanSplitRangeOps(uint64_t txn)
    {
        std::unique_lock x_lk(log_state_mutex_);
        auto split_range_op_it = tx_split_range_ops_.find(txn);
        if (split_range_op_it != tx_split_range_ops_.end())
        {
            tx_split_range_ops_.erase(split_range_op_it);
        }
    }

    uint32_t LatestCommittedTxnNumber() const
    {
        return cc_ng_info_.latest_txn_no_.load(std::memory_order_relaxed);
    }

    void UpdateLatestCommittedTxnNumber(uint32_t tx_ident)
    {
        // to handle the situation that committed txn number wraps around
        // uint32, assuming that active txn numbers won't span half of
        // UINT32_MAX
        if (tx_ident - cc_ng_info_.latest_txn_no_ < (UINT32_MAX >> 1))
        {
            cc_ng_info_.latest_txn_no_.store(tx_ident,
                                             std::memory_order_relaxed);
        }
    }

    void UpdateCkptTs(uint64_t timestamp)
    {
        std::unique_lock<std::shared_mutex> lk(log_state_mutex_);
        if (timestamp >
            cc_ng_info_.last_ckpt_ts_.load(std::memory_order_relaxed))
        {
            uint32_t max_txn =
                cc_ng_info_.latest_txn_no_.load(std::memory_order_relaxed);
            int rc = PersistCkptAndMaxTxn(timestamp, max_txn);
            while (rc != 0)
            {
                rc = PersistCkptAndMaxTxn(timestamp, max_txn);
            }

            cc_ng_info_.last_ckpt_ts_.store(timestamp,
                                            std::memory_order_release);
            TryCleanMultiStageOps();
        }
    }

    uint64_t LastCkptTimestamp()
    {
        return cc_ng_info_.last_ckpt_ts_.load(std::memory_order_relaxed);
    }

    CcNgInfo &GetCcNgInfo()
    {
        return cc_ng_info_;
    }

    virtual uint64_t GetApproximateReplayLogSize()
    {
        return 0;
    };

protected:
    void GetSchemaOpList(std::vector<Item::Pointer> &res)
    {
        for (const auto &[txn, catalog_op] : tx_catalog_ops_)
        {
            for (uint16_t idx = 0; idx < catalog_op.SchemaOpMsgCount(); ++idx)
            {
                const SchemaOpMessage &msg = catalog_op.SchemaOpMsgs()[idx];
                if (msg.stage() !=
                    SchemaOpMessage_Stage::SchemaOpMessage_Stage_CleanSchema)
                {
                    std::string schema_op_str;
                    msg.SerializeToString(&schema_op_str);
                    res.emplace_back(
                        std::make_shared<Item>(txn,
                                               catalog_op.CommitTs(),
                                               std::move(schema_op_str),
                                               LogItemType::SchemaLog));
                }
            }
        }
    }

    void GetSplitRangeOpList(std::vector<Item::Pointer> &res)
    {
        for (const auto &[txn, split_range_op] : tx_split_range_ops_)
        {
            if (split_range_op.split_range_op_message_.stage() ==
                SplitRangeOpMessage_Stage::SplitRangeOpMessage_Stage_CleanSplit)
            {
                continue;
            }
            std::string split_range_op_str;
            // Add table name firstly
            std::string table_name =
                split_range_op.split_range_op_message_.table_name();
            uint8_t tabname_len = table_name.length();
            const char *ptr = reinterpret_cast<const char *>(&tabname_len);
            split_range_op_str.append(ptr, sizeof(uint8_t));
            split_range_op_str.append(table_name.data(), tabname_len);
            // then, add split range op
            split_range_op.split_range_op_message_.AppendToString(
                &split_range_op_str);

            res.emplace_back(
                std::make_shared<Item>(txn,
                                       split_range_op.commit_ts_,
                                       std::move(split_range_op_str),
                                       LogItemType::SplitRangeLog));
        }
    }

    /**
     * The multi-stage logs at clean stage will be erased when all
     * node group's ckpt_ts are one hour greater than its commit ts.
     */
    void TryCleanMultiStageOps()
    {
        uint64_t ckpt_ts =
            cc_ng_info_.last_ckpt_ts_.load(std::memory_order_relaxed);
        using namespace std::chrono_literals;
        uint64_t one_hour = std::chrono::microseconds(1h).count();
        for (auto it = tx_catalog_ops_.begin(); it != tx_catalog_ops_.end();)
        {
            const CatalogOp &op = it->second;
            auto stage = op.SchemasOpStage();
            if (stage ==
                    SchemaOpMessage_Stage::SchemaOpMessage_Stage_CleanSchema &&
                ckpt_ts > op.CommitTs() + one_hour)
            {
                LOG(INFO) << "erasing schema op at clean stage after one hour, "
                             "commit_ts: "
                          << op.CommitTs() << ", ckpt ts: " << ckpt_ts;

                uint64_t txn_to_delete = it->first;
                uint64_t commit_ts_to_delete = op.CommitTs();
                it = tx_catalog_ops_.erase(it);

                int rc = DeleteSchemaOp(txn_to_delete, commit_ts_to_delete);
                while (rc != 0)
                {
                    rc = DeleteSchemaOp(txn_to_delete, commit_ts_to_delete);
                }
            }
            else
            {
                it++;
            }
        }
        for (auto it = tx_split_range_ops_.begin();
             it != tx_split_range_ops_.end();)
        {
            const SplitRangeOp &op = it->second;
            auto stage = op.split_range_op_message_.stage();
            if (stage == SplitRangeOpMessage_Stage::
                             SplitRangeOpMessage_Stage_CleanSplit &&
                ckpt_ts > op.commit_ts_ + one_hour)
            {
                LOG(INFO) << "erasing range split op at clean stage after one "
                             "hour, commit_ts: "
                          << op.commit_ts_ << ", ckpt ts: " << ckpt_ts;

                uint64_t txn_to_delete = it->first;
                uint64_t commit_ts_to_delete = op.commit_ts_;
                it = tx_split_range_ops_.erase(it);

                int rc = DeleteRangeOp(txn_to_delete, commit_ts_to_delete);
                while (rc != 0)
                {
                    rc = DeleteRangeOp(txn_to_delete, commit_ts_to_delete);
                }
            }
            else
            {
                it++;
            }
        }
    }

    virtual int PersistSchemaOp(uint64_t txn,
                                uint64_t timestamp,
                                const SchemaOpMessage &schema_op) = 0;

    virtual int PersistSchemasOp(
        uint64_t txn,
        uint64_t timestamp,
        const ::google::protobuf::RepeatedPtrField<SchemaOpMessage>
            &schemas_op) = 0;

    virtual int DeleteSchemaOp(uint64_t txn, uint64_t timestamp) = 0;

    virtual int PersistRangeOp(uint64_t txn,
                               uint64_t timestamp,
                               const SplitRangeOpMessage &range_op) = 0;

    virtual int DeleteRangeOp(uint64_t txn, uint64_t timestamp) = 0;

    virtual int PersistCkptAndMaxTxn(uint64_t ckpt_ts, uint32_t max_txn) = 0;

    CcNgInfo cc_ng_info_;

    struct CatalogOp
    {
        CatalogOp(const SchemaOpMessage &schema_op, uint64_t commit_ts)
            : schemas_op_msg_({schema_op}), commit_ts_(commit_ts)
        {
        }

        CatalogOp(SchemaOpMessage &&schema_op, uint64_t commit_ts)
            : schemas_op_msg_({std::move(schema_op)}), commit_ts_(commit_ts)
        {
        }

        CatalogOp(const ::google::protobuf::RepeatedPtrField<SchemaOpMessage>
                      &schemas_op,
                  uint64_t commit_ts)
            : schemas_op_msg_(schemas_op.begin(), schemas_op.end()),
              commit_ts_(commit_ts)
        {
        }

        CatalogOp(std::vector<SchemaOpMessage> schemas_op, uint64_t commit_ts)
            : schemas_op_msg_(std::move(schemas_op)), commit_ts_(commit_ts)
        {
        }
        uint64_t CommitTs() const
        {
            return commit_ts_;
        }

        void SetCommitTs(uint64_t commit_ts)
        {
            commit_ts_ = commit_ts;
        }

        // If contains only one SchemaOpMessage, return its stage.
        // If contains multiple SchemaOpMessage, return clean only when all
        // clean and return commit otherwise.
        SchemaOpMessage_Stage SchemasOpStage() const
        {
            if (schemas_op_msg_.size() == 1)
            {
                return schemas_op_msg_.front().stage();
            }
            else
            {
                bool all_cleaned =
                    std::all_of(schemas_op_msg_.begin(),
                                schemas_op_msg_.end(),
                                [](const SchemaOpMessage &schema_op_msg)
                                {
                                    return schema_op_msg.stage() ==
                                           SchemaOpMessage_Stage_CleanSchema;
                                });
                if (all_cleaned)
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_CleanSchema;
                }

                bool all_committed =
                    std::all_of(schemas_op_msg_.begin(),
                                schemas_op_msg_.end(),
                                [](const SchemaOpMessage &schema_op_msg)
                                {
                                    return schema_op_msg.stage() ==
                                           SchemaOpMessage_Stage_CommitSchema;
                                });
                if (all_committed)
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_CommitSchema;
                }
                else
                {
                    return SchemaOpMessage_Stage::
                        SchemaOpMessage_Stage_PrepareSchema;
                }
            }
        }

        void CommitAll()
        {
            for (SchemaOpMessage &schema_op_msg : schemas_op_msg_)
            {
                schema_op_msg.set_stage(SchemaOpMessage_Stage_CommitSchema);
            }
        }

        void ClearAll()
        {
            for (SchemaOpMessage &schema_op_msg : schemas_op_msg_)
            {
                schema_op_msg.Clear();
                schema_op_msg.set_stage(SchemaOpMessage_Stage_CleanSchema);
            }
        }

        void Clear(const std::string &table_name, CcTableType table_type)
        {
            auto it = std::find_if(
                schemas_op_msg_.begin(),
                schemas_op_msg_.end(),
                [&table_name, table_type](const SchemaOpMessage &schema_op_msg)
                {
                    return schema_op_msg.table_name_str() == table_name &&
                           schema_op_msg.table_type() == table_type;
                });
            assert(it != schemas_op_msg_.end());
            SchemaOpMessage &schema_op_msg = *it;
            schema_op_msg.Clear();
            schema_op_msg.set_stage(SchemaOpMessage_Stage_CleanSchema);
        }

        SchemaOpMessage *GetSchemaOpMsg(const std::string &table_name,
                                        CcTableType table_type)
        {
            auto it = std::find_if(
                schemas_op_msg_.begin(),
                schemas_op_msg_.end(),
                [&table_name, table_type](const SchemaOpMessage &schema_op_msg)
                {
                    return schema_op_msg.table_name_str() == table_name &&
                           schema_op_msg.table_type() == table_type;
                });

            if (it != schemas_op_msg_.end())
            {
                return std::addressof(*it);
            }
            else
            {
                return nullptr;
            }
        }

        const SchemaOpMessage *SchemaOpMsgs() const
        {
            return schemas_op_msg_.data();
        }

        SchemaOpMessage *SchemaOpMsgs()
        {
            return schemas_op_msg_.data();
        }

        size_t SchemaOpMsgCount() const
        {
            return schemas_op_msg_.size();
        }

    private:
        // DML-trigger-DDL allows update to multiple catalogs.
        // pure-DDL allows update to only one catalog.
        std::vector<SchemaOpMessage> schemas_op_msg_;
        uint64_t commit_ts_;
    };

    /**
     * @brief A collection ongoing tx's and their ongoing schema operations.
     *
     */
    std::unordered_map<uint64_t, CatalogOp> tx_catalog_ops_;
    std::unordered_map<uint64_t, CatalogOp> snapshot_tx_catalog_ops_;

    /**
     * protects concurrent access to log state, more specifically, CcNode
     * group term and last_ckpt_ts and tx_catalog_ops_. RecoverTx reads them
     * and state machine on_apply modifies them. Since RecoverTx is
     * processed in separate RPC thread, they are concurrent.
     */
    mutable std::shared_mutex log_state_mutex_;

    struct SplitRangeOp
    {
        SplitRangeOp(const SplitRangeOpMessage &split_range_op_message,
                     uint64_t commit_ts)
            : split_range_op_message_(split_range_op_message),
              commit_ts_(commit_ts)
        {
        }

        SplitRangeOp(SplitRangeOpMessage &split_range_op_message,
                     uint64_t commit_ts)
            : split_range_op_message_(split_range_op_message),
              commit_ts_(commit_ts)
        {
        }

        SplitRangeOpMessage split_range_op_message_;
        uint64_t commit_ts_;
    };

    /**
     * @brief A collection ongoint tx's and their ongoing split range
     * operations.
     *
     */
    std::unordered_map<uint64_t, SplitRangeOp> tx_split_range_ops_;
    std::unordered_map<uint64_t, SplitRangeOp> snapshot_tx_split_range_ops_;

    struct ClusterScaleOp
    {
        ClusterScaleOp(const ClusterScaleOpMessage &cluster_scale_op_message,
                       uint64_t commit_ts)
            : cluster_scale_op_message_(cluster_scale_op_message),
              commit_ts_(commit_ts)
        {
        }

        ClusterScaleOp(ClusterScaleOpMessage &cluster_scale_op_message,
                       uint64_t commit_ts)
            : cluster_scale_op_message_(cluster_scale_op_message),
              commit_ts_(commit_ts)
        {
        }

        ClusterScaleOpMessage cluster_scale_op_message_;
        uint64_t commit_ts_;
    };
};
}  // namespace txlog
