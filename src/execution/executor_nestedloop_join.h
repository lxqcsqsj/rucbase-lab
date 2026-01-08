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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);

    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return isend; }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    void beginTuple() override {
        isend = false;
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true;
            return;
        }
        right_->beginTuple();
        if (right_->is_end()) {
            isend = true;
            return;
        }

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

        auto curr_match = [&]() -> bool {
            auto l = left_->Next();
            auto r = right_->Next();
            if (l == nullptr || r == nullptr) return false;
            RmRecord joined(static_cast<int>(len_));
            memcpy(joined.data, l->data, left_->tupleLen());
            memcpy(joined.data + left_->tupleLen(), r->data, right_->tupleLen());
            return eval_conds(joined);
        };

        //找到第一对满足连接条件的记录
        while (!left_->is_end()) {
            while (!right_->is_end()) {
                if (fed_conds_.empty() || curr_match()) {
                    return;     //找到匹配对
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) break;
            right_->beginTuple();       //重置右表迭代器
        }
        isend = true;
    }

    void nextTuple() override {
        if (isend) return;

        right_->nextTuple();

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

        auto curr_match = [&]() -> bool {
            auto l = left_->Next();
            auto r = right_->Next();
            if (l == nullptr || r == nullptr) return false;
            RmRecord joined(static_cast<int>(len_));
            memcpy(joined.data, l->data, left_->tupleLen());
            memcpy(joined.data + left_->tupleLen(), r->data, right_->tupleLen());
            return eval_conds(joined);
        };

        while (!left_->is_end()) {
            while (!right_->is_end()) {
                if (fed_conds_.empty() || curr_match()) {
                    return;
                }
                right_->nextTuple();
            }
            left_->nextTuple();
            if (left_->is_end()) break;
            right_->beginTuple();
        }
        isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (isend) {
            return nullptr;
        }
        auto l = left_->Next();
        auto r = right_->Next();
        if (l == nullptr || r == nullptr) {
            return nullptr;
        }
        //拼接左右记录
        auto rec = std::make_unique<RmRecord>(static_cast<int>(len_));
        memcpy(rec->data, l->data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), r->data, right_->tupleLen());
        return rec;
    }

    Rid &rid() override { return _abstract_rid; }
};