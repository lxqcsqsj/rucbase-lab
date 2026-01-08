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

#include <climits>
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    void beginTuple() override {
        // 申请IS意向锁（表级）
        if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
            int tab_fd = fh_->GetFd();
            if (!context_->lock_mgr_->lock_IS_on_table(context_->txn_, tab_fd)) {
                throw std::runtime_error("Failed to acquire IS lock on table");
            }
        }
        
        // 使用索引进行扫描，并在键空间上加“间隙共享锁”，防止幻读
        // 如果有WHERE条件匹配索引列，使用等值或范围扫描
        // 如果没有WHERE条件，使用索引进行全表扫描（保证输出顺序一致）
        if (!index_col_names_.empty()) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
            
            // 初始化扫描范围为全表
            Iid lower = ih->leaf_begin();
            Iid upper = ih->leaf_end();
            
            // 计算间隙锁的范围（left_key, right_key）
            int left_key = INT_MIN;
            int right_key = INT_MAX;
            bool has_range = false;
            
            // 检查第一个索引列是否有范围条件（支持单列索引的范围查询，如 id > 2 and id < 4）
            if (index_meta_.cols.size() == 1 && index_meta_.cols[0].type == TYPE_INT) {
                const std::string& first_col_name = index_meta_.cols[0].name;
                char* range_key = new char[index_meta_.cols[0].len];
                
                for (auto &cond : conds_) {
                    if (cond.is_rhs_val && cond.lhs_col.tab_name == tab_name_ && 
                        cond.lhs_col.col_name == first_col_name) {
                        memcpy(range_key, cond.rhs_val.raw->data, index_meta_.cols[0].len);
                        int key_val = *reinterpret_cast<int*>(range_key);
                        
                        if (cond.op == OP_EQ) {
                            // 等值查询：锁住 [key, key] 区间
                            left_key = key_val;
                            right_key = key_val;
                            has_range = true;
                            lower = ih->lower_bound(range_key);
                            upper = ih->upper_bound(range_key);
                            break;
                        } else if (cond.op == OP_GT) {
                            // id > key: 从第一个 > key 的位置开始
                            left_key = key_val + 1;  // 不包含key本身
                            has_range = true;
                            lower = ih->upper_bound(range_key);
                        } else if (cond.op == OP_GE) {
                            // id >= key: 从第一个 >= key 的位置开始
                            left_key = key_val;  // 包含key本身
                            has_range = true;
                            lower = ih->lower_bound(range_key);
                        } else if (cond.op == OP_LT) {
                            // id < key: 到第一个 >= key 的位置结束（不包含）
                            right_key = key_val - 1;  // 不包含key本身
                            has_range = true;
                            upper = ih->lower_bound(range_key);
                        } else if (cond.op == OP_LE) {
                            // id <= key: 到第一个 > key 的位置结束（不包含）
                            right_key = key_val;  // 包含key本身
                            has_range = true;
                            upper = ih->upper_bound(range_key);
                        }
                    }
                }
                delete[] range_key;
            } else {
                // 多列索引：检查是否有等值条件匹配所有索引列
                bool has_eq_cond = true;
                std::vector<char> key(index_meta_.col_tot_len, 0);
                int off = 0;
                for (auto &col : index_meta_.cols) {
                    bool found = false;
                    for (auto &cond : conds_) {
                        if (cond.is_rhs_val && cond.op == OP_EQ &&
                            cond.lhs_col.tab_name == tab_name_ && cond.lhs_col.col_name == col.name) {
                            memcpy(key.data() + off, cond.rhs_val.raw->data, col.len);
                            off += col.len;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        has_eq_cond = false;
                        break;
                    }
                }
                
                if (has_eq_cond && off == index_meta_.col_tot_len) {
                    // 有等值条件，使用等值扫描
                    // 对于多列索引，暂时锁住整个表范围（简化处理）
                    has_range = true;
                    lower = ih->lower_bound(key.data());
                    upper = ih->upper_bound(key.data());
                }
                // 否则使用全表扫描（lower和upper已经是leaf_begin和leaf_end）
            }
            
            // 加间隙共享锁：锁住查询范围内的间隙，防止其他事务在该范围内插入/删除
            if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
                int tab_fd = fh_->GetFd();
                if (has_range) {
                    // 有明确的查询范围，锁住该范围
                    if (!context_->lock_mgr_->lock_shared_on_gap(context_->txn_, tab_fd, left_key, right_key)) {
                        throw std::runtime_error("Failed to acquire shared gap lock");
                    }
                } else {
                    // 全表扫描，锁住整个键空间
                    if (!context_->lock_mgr_->lock_shared_on_gap(context_->txn_, tab_fd, INT_MIN, INT_MAX)) {
                        throw std::runtime_error("Failed to acquire shared gap lock");
                    }
                }
            }
            
            scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        } else {
            // 没有索引，退化为顺序扫描（使用表级S锁防止幻读）
            if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
                int tab_fd = fh_->GetFd();
                if (!context_->lock_mgr_->lock_shared_on_table(context_->txn_, tab_fd)) {
                    throw std::runtime_error("Failed to acquire shared lock on table");
                }
            }
            scan_ = std::make_unique<RmScan>(fh_);
        }

        // 前进到第一个满足所有谓词的记录
        auto cmp = [](ColType type, const char *lhs, const char *rhs, int len) -> int {
            switch (type) {
                case TYPE_INT: {
                    int a = *reinterpret_cast<const int *>(lhs);
                    int b = *reinterpret_cast<const int *>(rhs);
                    return (a < b) ? -1 : ((a > b) ? 1 : 0);
                }
                case TYPE_FLOAT: {
                    float a = *reinterpret_cast<const float *>(lhs);
                    float b = *reinterpret_cast<const float *>(rhs);
                    return (a < b) ? -1 : ((a > b) ? 1 : 0);
                }
                case TYPE_STRING:
                    return memcmp(lhs, rhs, len);
                default:
                    throw InternalError("Unexpected data type");
            }
        };

        auto eval_cond = [&](const Condition &cond, const RmRecord &rec) -> bool {
            auto lhs_it = get_col(cols_, cond.lhs_col);
            const auto &lhs = *lhs_it;
            const char *lhs_ptr = rec.data + lhs.offset;
            const char *rhs_ptr = nullptr;
            if (cond.is_rhs_val) {
                rhs_ptr = cond.rhs_val.raw->data;
            } else {
                auto rhs_it = get_col(cols_, cond.rhs_col);
                rhs_ptr = rec.data + rhs_it->offset;
            }
            int c = cmp(lhs.type, lhs_ptr, rhs_ptr, lhs.len);
            switch (cond.op) {
                case OP_EQ: return c == 0;
                case OP_NE: return c != 0;
                case OP_LT: return c < 0;
                case OP_GT: return c > 0;
                case OP_LE: return c <= 0;
                case OP_GE: return c >= 0;
                default: throw InternalError("Unexpected comparison operator");
            }
        };

        auto eval_conds = [&](const RmRecord &rec) -> bool {
            for (auto &cond : fed_conds_) {
                if (!eval_cond(cond, rec)) return false;
            }
            return true;
        };

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (rec != nullptr && eval_conds(*rec)) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        if (scan_ == nullptr || scan_->is_end()) {
            return;
        }
        scan_->next();

        auto cmp = [](ColType type, const char *lhs, const char *rhs, int len) -> int {
            switch (type) {
                case TYPE_INT: {
                    int a = *reinterpret_cast<const int *>(lhs);
                    int b = *reinterpret_cast<const int *>(rhs);
                    return (a < b) ? -1 : ((a > b) ? 1 : 0);
                }
                case TYPE_FLOAT: {
                    float a = *reinterpret_cast<const float *>(lhs);
                    float b = *reinterpret_cast<const float *>(rhs);
                    return (a < b) ? -1 : ((a > b) ? 1 : 0);
                }
                case TYPE_STRING:
                    return memcmp(lhs, rhs, len);
                default:
                    throw InternalError("Unexpected data type");
            }
        };

        auto eval_cond = [&](const Condition &cond, const RmRecord &rec) -> bool {
            auto lhs_it = get_col(cols_, cond.lhs_col);
            const auto &lhs = *lhs_it;
            const char *lhs_ptr = rec.data + lhs.offset;
            const char *rhs_ptr = nullptr;
            if (cond.is_rhs_val) {
                rhs_ptr = cond.rhs_val.raw->data;
            } else {
                auto rhs_it = get_col(cols_, cond.rhs_col);
                rhs_ptr = rec.data + rhs_it->offset;
            }
            int c = cmp(lhs.type, lhs_ptr, rhs_ptr, lhs.len);
            switch (cond.op) {
                case OP_EQ: return c == 0;
                case OP_NE: return c != 0;
                case OP_LT: return c < 0;
                case OP_GT: return c > 0;
                case OP_LE: return c <= 0;
                case OP_GE: return c >= 0;
                default: throw InternalError("Unexpected comparison operator");
            }
        };

        auto eval_conds = [&](const RmRecord &rec) -> bool {
            for (auto &cond : fed_conds_) {
                if (!eval_cond(cond, rec)) return false;
            }
            return true;
        };

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (rec != nullptr && eval_conds(*rec)) {
                return;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
};