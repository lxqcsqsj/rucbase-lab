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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
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
        
        // Update each rid from record file and index file
        for (auto& rid : rids_) {
            // 先尝试申请X锁（如果已经持有S锁，会尝试升级为X锁）
            // 这样可以避免先申请S锁再升级的问题
            if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
                int tab_fd = fh_->GetFd();
                if (!context_->lock_mgr_->lock_exclusive_on_record(context_->txn_, rid, tab_fd)) {
                    throw std::runtime_error("Failed to acquire exclusive lock on record");
                }
            }
            
            // 现在已经有X锁了，可以安全地读取记录
            // get_record会尝试申请S锁，但由于我们已经持有X锁，应该可以直接读取
            auto rec = fh_->get_record(rid, context_);
            RmRecord record = *rec;
            for (auto& set_clause : set_clauses_) {
                auto lhs_col = tab_.get_col(set_clause.lhs.col_name);
                memcpy(rec->data + lhs_col->offset, set_clause.rhs.raw->data, lhs_col->len);
            }
            // record a update operation into the transaction (must be before modifying index/record)
            WriteRecord* wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, record);
            context_->txn_->append_write_record(wr);
            
            // Remove old entry from index and record index undo log
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* old_key = new char[index.col_tot_len];
                int offset = 0;
                for (int j = 0; j < index.col_num; ++j) {
                    memcpy(old_key + offset, record.data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                
                // 对于单列INT索引，加排它间隙锁：更新操作会改变键空间
                if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr &&
                    index.col_num == 1 && index.cols[0].type == TYPE_INT) {
                    int tab_fd = fh_->GetFd();
                    int old_key_val = *reinterpret_cast<int*>(old_key);
                    // 锁住旧key的间隙
                    if (!context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, tab_fd, old_key_val, old_key_val)) {
                        delete[] old_key;
                        throw std::runtime_error("Failed to acquire exclusive gap lock for update (old key)");
                    }
                }
                
                // 删除旧索引条目
                ih->delete_entry(old_key, context_->txn_);
                
                // 记录索引删除的 undo log：如果事务 abort，需要恢复这个索引条目
                wr->AddIndexOp(index.cols, old_key, index.col_tot_len, rid, IndexOpType::INDEX_DELETE);
                
                delete[] old_key;
            }
            // Update record in record file
            fh_->update_record(rid, rec->data, context_);
            // Insert new index into index and record index undo log
            for (size_t i = 0; i < tab_.indexes.size(); ++i) {
                auto& index = tab_.indexes[i];
                auto ih =
                    sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char* new_key = new char[index.col_tot_len];
                int offset = 0;
                for (int j = 0; j < index.col_num; ++j) {
                    memcpy(new_key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                
                // 对于单列INT索引，如果新key和旧key不同，也需要锁住新key的间隙
                if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr &&
                    index.col_num == 1 && index.cols[0].type == TYPE_INT) {
                    int tab_fd = fh_->GetFd();
                    int new_key_val = *reinterpret_cast<int*>(new_key);
                    // 检查新key是否与旧key不同（更新了索引列）
                    char* old_key_check = new char[index.col_tot_len];
                    int offset_check = 0;
                    for (int j = 0; j < index.col_num; ++j) {
                        memcpy(old_key_check + offset_check, record.data + index.cols[j].offset, index.cols[j].len);
                        offset_check += index.cols[j].len;
                    }
                    int old_key_val = *reinterpret_cast<int*>(old_key_check);
                    delete[] old_key_check;
                    
                    if (new_key_val != old_key_val) {
                        // 新key和旧key不同，需要锁住新key的间隙
                        if (!context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, tab_fd, new_key_val, new_key_val)) {
                            delete[] new_key;
                            throw std::runtime_error("Failed to acquire exclusive gap lock for update (new key)");
                        }
                    }
                }
                
                // 插入新索引条目
                ih->insert_entry(new_key, rid, context_->txn_);
                
                // 记录索引插入的 undo log：如果事务 abort，需要删除这个索引条目
                wr->AddIndexOp(index.cols, new_key, index.col_tot_len, rid, IndexOpType::INDEX_INSERT);
                
                delete[] new_key;
            }
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};