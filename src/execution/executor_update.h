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
        for (auto &rid : rids_) {
            auto old_rec = fh_->get_record(rid, context_);
            if (old_rec == nullptr) continue;

            // 构造新 record（基于旧 record 修改 set 子句）
            RmRecord new_rec(*old_rec);
            for (auto &set_clause : set_clauses_) {
                auto col_it = tab_.get_col(set_clause.lhs.col_name);
                memcpy(new_rec.data + col_it->offset, set_clause.rhs.raw->data, col_it->len);
            }

            // 更新所有索引：删除旧 key，插入新 key（如有变化）
            for (auto &index : tab_.indexes) {
                std::vector<char> old_key(index.col_tot_len);
                std::vector<char> new_key(index.col_tot_len);
                int off = 0;
                for (int i = 0; i < index.col_num; i++) {
                    auto &col = index.cols[i];
                    memcpy(old_key.data() + off, old_rec->data + col.offset, col.len);
                    memcpy(new_key.data() + off, new_rec.data + col.offset, col.len);
                    off += col.len;
                }
                if (memcmp(old_key.data(), new_key.data(), index.col_tot_len) != 0) {
                    auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                    ih->delete_entry(old_key.data(), context_ ? context_->txn_ : nullptr);
                    ih->insert_entry(new_key.data(), rid, context_ ? context_->txn_ : nullptr);
                }
            }

            fh_->update_record(rid, new_rec.data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};