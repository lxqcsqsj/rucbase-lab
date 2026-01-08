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

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // 申请IX意向锁（表级）
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            int tab_fd = fh_->GetFd();
            if (!context_->lock_mgr_->lock_IX_on_table(context_->txn_, tab_fd)) {
                throw std::runtime_error("Failed to acquire IX lock on table");
            }
        }
        
        // Make record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }
        // Insert into record file
        rid_ = fh_->insert_record(rec.data, context_);
        // record a insert operation into the transaction
        // 保存记录数据，以便回滚时能够删除索引
        WriteRecord *wr = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_, rec);
        context_->txn_->append_write_record(wr);
        // Insert into index and record index undo log
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (int j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            
            // 对于单列INT索引，加排它间隙锁：检查插入的key是否落在被锁的间隙中
            if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr &&
                index.col_num == 1 && index.cols[0].type == TYPE_INT) {
                int tab_fd = fh_->GetFd();
                int insert_key = *reinterpret_cast<int*>(key);
                // 尝试获取排它间隙锁：锁住插入key所在的间隙
                // 如果该间隙已被其他事务的共享间隙锁占用，会冲突并abort
                if (!context_->lock_mgr_->lock_exclusive_on_gap(context_->txn_, tab_fd, insert_key, insert_key)) {
                    delete[] key;
                    throw std::runtime_error("Failed to acquire exclusive gap lock for insert");
                }
            }
            
            // 插入索引条目
            ih->insert_entry(key, rid_, context_->txn_);
            
            // 记录索引插入的 undo log：如果事务 abort，需要删除这个索引条目
            wr->AddIndexOp(index.cols, key, index.col_tot_len, rid_, IndexOpType::INDEX_INSERT);
            
            delete[] key;
        }
        return nullptr;
    }
    Rid &rid() override { return rid_; }
};