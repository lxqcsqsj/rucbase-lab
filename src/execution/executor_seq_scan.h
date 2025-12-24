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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    bool is_end() const override { return scan_ == nullptr || scan_->is_end(); }

    ColMeta get_col_offset(const TabCol &target) override { return *get_col(cols_, target); }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);

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

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
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

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        if (is_end()) {
            return nullptr;
        }
        return fh_->get_record(rid_, context_);
    }

    Rid &rid() override { return rid_; }
};