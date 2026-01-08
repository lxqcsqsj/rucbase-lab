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

#include <atomic>
#include <vector>
#include <cstring>

#include "common/config.h"
#include "defs.h"
#include "record/rm_defs.h"
#include "system/sm_meta.h"

/* 标识事务状态 */
enum class TransactionState { DEFAULT, GROWING, SHRINKING, COMMITTED, ABORTED };

/* 系统的隔离级别，当前赛题中为可串行化隔离级别 */
enum class IsolationLevel { READ_UNCOMMITTED, REPEATABLE_READ, READ_COMMITTED, SERIALIZABLE };

/* 事务写操作类型，包括插入、删除、更新三种操作 */
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE, UPDATE_TUPLE};

/* 索引操作类型 */
enum class IndexOpType { INDEX_INSERT = 0, INDEX_DELETE };

/**
 * @brief 索引操作的 undo log 记录
 */
struct IndexWriteRecord {
    std::vector<ColMeta> index_cols;  // 索引包含的列，用于获取索引句柄
    char* key;                         // 索引键值（动态分配）
    size_t key_len;                    // 键值长度
    Rid rid;                           // 记录的位置
    IndexOpType op_type;               // 操作类型：INSERT 或 DELETE
    
    IndexWriteRecord() : key(nullptr), key_len(0) {}
    
    IndexWriteRecord(const std::vector<ColMeta>& cols, char* k, size_t len, const Rid& r, IndexOpType op)
        : index_cols(cols), key(k), key_len(len), rid(r), op_type(op) {}
    
    ~IndexWriteRecord() {
        if (key != nullptr) {
            delete[] key;
            key = nullptr;
        }
    }
    
    // 禁止拷贝构造和赋值，避免重复释放内存
    IndexWriteRecord(const IndexWriteRecord&) = delete;
    IndexWriteRecord& operator=(const IndexWriteRecord&) = delete;
    
    // 移动构造和移动赋值
    IndexWriteRecord(IndexWriteRecord&& other) noexcept
        : index_cols(std::move(other.index_cols)), key(other.key), key_len(other.key_len), 
          rid(other.rid), op_type(other.op_type) {
        other.key = nullptr;
        other.key_len = 0;
    }
    
    IndexWriteRecord& operator=(IndexWriteRecord&& other) noexcept {
        if (this != &other) {
            if (key != nullptr) {
                delete[] key;
            }
            index_cols = std::move(other.index_cols);
            key = other.key;
            key_len = other.key_len;
            rid = other.rid;
            op_type = other.op_type;
            other.key = nullptr;
            other.key_len = 0;
        }
        return *this;
    }
};

/**
 * @brief 事务的写操作记录，用于事务的回滚
 * INSERT
 * --------------------------------
 * | wtype | tab_name | tuple_rid |
 * --------------------------------
 * DELETE / UPDATE
 * ----------------------------------------------
 * | wtype | tab_name | tuple_rid | tuple_value |
 * ----------------------------------------------
 */
class WriteRecord {
   public:
    WriteRecord() = default;

    // constructor for insert operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid) {}

    // constructor for delete & update operation
    WriteRecord(WType wtype, const std::string &tab_name, const Rid &rid, const RmRecord &record)
        : wtype_(wtype), tab_name_(tab_name), rid_(rid), record_(record) {}

    ~WriteRecord() {
        // 清理索引操作的 key 内存
        for (auto& idx_op : index_ops_) {
            if (idx_op.key != nullptr) {
                delete[] idx_op.key;
                idx_op.key = nullptr;
            }
        }
    }

    inline RmRecord &GetRecord() { return record_; }

    inline Rid &GetRid() { return rid_; }

    inline WType &GetWriteType() { return wtype_; }

    inline std::string &GetTableName() { return tab_name_; }
    
    // 添加索引操作记录
    void AddIndexOp(const std::vector<ColMeta>& index_cols, char* key, size_t key_len, 
                    const Rid& rid, IndexOpType op_type) {
        // 分配新的 key 内存并复制
        char* new_key = new char[key_len];
        memcpy(new_key, key, key_len);
        index_ops_.emplace_back(index_cols, new_key, key_len, rid, op_type);
    }
    
    // 获取索引操作列表
    inline std::vector<IndexWriteRecord>& GetIndexOps() { return index_ops_; }

   private:
    WType wtype_;
    std::string tab_name_;
    Rid rid_;
    RmRecord record_;
    std::vector<IndexWriteRecord> index_ops_;  // 索引操作的 undo log
};

/* 多粒度锁，加锁对象的类型，包括表、记录和间隙（GAP） */
enum class LockDataType { TABLE = 0, RECORD = 1, GAP = 2 };

/**
 * @description: 加锁对象的唯一标识
 */
class LockDataId {
   public:
    /* 表级锁 */
    LockDataId(int fd, LockDataType type) {
        assert(type == LockDataType::TABLE);
        fd_ = fd;
        type_ = type;
        rid_.page_no = -1;
        rid_.slot_no = -1;
    }

    /* 行级锁 / 间隙锁：非表级的粒度都复用 (fd, rid) 作为唯一标识 */
    LockDataId(int fd, const Rid &rid, LockDataType type) {
        assert(type != LockDataType::TABLE);
        fd_ = fd;
        rid_ = rid;
        type_ = type;
    }

    inline int64_t Get() const {
        if (type_ == LockDataType::TABLE) {
            // 表级锁：仅根据fd区分
            return static_cast<int64_t>(fd_);
        } else if (type_ == LockDataType::RECORD) {
            // 行级锁：fd_, rid_.page_no, rid_.slot_no
            return ((static_cast<int64_t>(type_)) << 63) | ((static_cast<int64_t>(fd_)) << 31) |
                   ((static_cast<int64_t>(rid_.page_no)) << 16) | rid_.slot_no;
        } else {  // LockDataType::GAP
            // 间隙锁：同一张表上的所有gap共用一个锁ID（退化为表级间隙锁，简化冲突检测）
            return ((static_cast<int64_t>(type_)) << 63) | (static_cast<int64_t>(fd_) << 31);
        }
    }

    bool operator==(const LockDataId &other) const {
        if (type_ != other.type_) return false;
        if (fd_ != other.fd_) return false;
        if (type_ == LockDataType::GAP) {
            // 所有gap视为同一资源
            return true;
        }
        return rid_ == other.rid_;
    }
    int fd_;
    Rid rid_;
    LockDataType type_;
};

template <>
struct std::hash<LockDataId> {
    size_t operator()(const LockDataId &obj) const { return std::hash<int64_t>()(obj.Get()); }
};

/* 事务回滚原因 */
enum class AbortReason { LOCK_ON_SHIRINKING = 0, UPGRADE_CONFLICT, DEADLOCK_PREVENTION };

/* 事务回滚异常，在rmdb.cpp中进行处理 */
class TransactionAbortException : public std::exception {
    txn_id_t txn_id_;
    AbortReason abort_reason_;

   public:
    explicit TransactionAbortException(txn_id_t txn_id, AbortReason abort_reason)
        : txn_id_(txn_id), abort_reason_(abort_reason) {}

    txn_id_t get_transaction_id() { return txn_id_; }
    AbortReason GetAbortReason() { return abort_reason_; }
    std::string GetInfo() {
        switch (abort_reason_) {
            case AbortReason::LOCK_ON_SHIRINKING: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because it cannot request locks on SHRINKING phase\n";
            } break;

            case AbortReason::UPGRADE_CONFLICT: {
                return "Transaction " + std::to_string(txn_id_) +
                       " aborted because another transaction is waiting for upgrading\n";
            } break;

            case AbortReason::DEADLOCK_PREVENTION: {
                return "Transaction " + std::to_string(txn_id_) + " aborted for deadlock prevention\n";
            } break;

            default: {
                return "Transaction aborted\n";
            } break;
        }
    }
};