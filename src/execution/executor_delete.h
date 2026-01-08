/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class DeleteExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Condition> conds_;  // delete的条件
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::vector<Rid> rids_;         // 需要删除的记录的位置
    std::string tab_name_;          // 表名称
    SmManager *sm_manager_;

   public:
    DeleteExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Condition> conds,
                   std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }

    std::unique_ptr<RmRecord> Next() override {
        // 申请IX意向锁（表级）
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            int tab_fd = fh_->GetFd();
            if (!context_->lock_mgr_->lock_IX_on_table(context_->txn_, tab_fd)) {
                throw std::runtime_error("Failed to acquire IX lock on table");
            }
        }
        
        for (Rid &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);
            // record a delete operation into the transaction (must be before deleting index/record)
            WriteRecord *wr = new WriteRecord(WType::DELETE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);
            // Delete index and record index undo log
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto &index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (int j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                
                // 对于单列INT索引，加排它间隙锁：删除操作会改变键空间
                if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr &&
                    index.col_num == 1 && index.cols[0].type == TYPE_INT) {
                    int tab_fd = fh_->GetFd();
                    int delete_key = *reinterpret_cast<int*>(key);
                    // 尝试获取排它间隙锁
                    if (!context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, tab_fd, delete_key, delete_key)) {
                        delete[] key;
                        throw std::runtime_error("Failed to acquire exclusive gap lock for delete");
                    }
                }
                
                // 删除索引条目
                ih->delete_entry(key, context_->txn_);
                
                // 记录索引删除的 undo log：如果事务 abort，需要恢复这个索引条目
                wr->AddIndexOp(index.cols, key, index.col_tot_len, rid, IndexOpType::INDEX_DELETE);
                
                delete[] key;
            }
            // Delete record file
            fh_->delete_record(rid, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};