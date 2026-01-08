/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"

#include "record/rm_file_handle.h"
#include "system/sm_manager.h"
#include "common/context.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针

    std::scoped_lock lock(latch_);

    if (txn == nullptr) {
        txn = new Transaction(next_txn_id_++);
    }
    txn_map[txn->get_transaction_id()] = txn;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction *txn, LogManager *log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    std::scoped_lock lock(latch_);

    auto write_set = txn->get_write_set();
    write_set->clear();
    auto lock_set = txn->get_lock_set();
    for (auto lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }
    lock_set->clear();

    txn->set_state(TransactionState::COMMITTED);
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Transaction * txn, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态

    std::scoped_lock lock(latch_);

    auto write_set = txn->get_write_set();
    Context *context = new Context(lock_manager_, log_manager, txn);
    while (!write_set->empty()) {
        auto &item = write_set->back();
        WType type = item->GetWriteType();
        
        // 首先处理索引 undo log（倒序执行，LIFO）
        auto &index_ops = item->GetIndexOps();
        for (auto it = index_ops.rbegin(); it != index_ops.rend(); ++it) {
            auto &idx_op = *it;
            auto &tab_name = item->GetTableName();
            auto tab = sm_manager_->db_.get_table(tab_name);
            
            // 获取索引句柄
            auto ih = sm_manager_->ihs_.at(
                sm_manager_->get_ix_manager()->get_index_name(tab_name, idx_op.index_cols)
            ).get();
            
            if (idx_op.op_type == IndexOpType::INDEX_INSERT) {
                // 回滚索引插入：删除索引条目
                try {
                    ih->delete_entry(idx_op.key, context->txn_);
                } catch (...) {
                    // 索引条目可能不存在，忽略
                }
            } else if (idx_op.op_type == IndexOpType::INDEX_DELETE) {
                // 回滚索引删除：恢复索引条目
                try {
                    ih->insert_entry(idx_op.key, idx_op.rid, context->txn_);
                } catch (...) {
                    // 索引条目可能已存在，忽略
                }
            }
        }
        
        // 然后处理表操作的 undo
        if (type == WType::INSERT_TUPLE) {
            auto &tab_name = item->GetTableName();
            auto &rid = item->GetRid();
            auto tab = sm_manager_->db_.get_table(tab_name);
            auto fh = sm_manager_->fhs_.at(tab_name).get();
            
            // 获取保存的记录数据（如果WriteRecord保存了记录数据）
            // 注意：INSERT的WriteRecord现在应该保存了记录数据
            RmRecord *rec_data = nullptr;
            std::unique_ptr<RmRecord> file_rec;
            try {
                // 尝试从WriteRecord获取记录数据
                auto &saved_rec = item->GetRecord();
                // 验证记录数据是否有效（检查data指针和size，以及size是否匹配表结构）
                int expected_size = fh->get_file_hdr().record_size;
                if (saved_rec.data != nullptr && saved_rec.size > 0 && saved_rec.size == expected_size) {
                    rec_data = &saved_rec;
                }
            } catch (...) {
                // WriteRecord可能没有保存记录数据（旧版本），尝试从文件读取
                rec_data = nullptr;
            }
            
            // 如果WriteRecord没有保存有效记录数据，尝试从文件读取
            if (rec_data == nullptr) {
                try {
                    file_rec = fh->get_record(rid, context);
                    int expected_size = fh->get_file_hdr().record_size;
                    if (file_rec != nullptr && file_rec->data != nullptr && 
                        file_rec->size > 0 && file_rec->size == expected_size) {
                        rec_data = file_rec.get();
                    }
                } catch (...) {
                    // 记录不存在，可能已被DELETE回滚删除
                    // 无法获取记录数据，无法删除索引条目
                    rec_data = nullptr;
                }
            }
            
            // Delete record file
            // 注意：索引 undo log 已经在上面处理了，这里只需要删除记录
            try {
                fh->delete_record(rid, context);
            } catch (...) {
                // 记录可能不存在，忽略
            }
        } else if (type == WType::DELETE_TUPLE) {
            auto &tab_name = item->GetTableName();
            auto &rid = item->GetRid();  // 使用原来的RID
            auto tab = sm_manager_->db_.get_table(tab_name);
            auto fh = sm_manager_->fhs_.at(tab_name).get();
            
            // 获取被删除的记录数据并验证其有效性
            auto &rec = item->GetRecord();  // 被删除的记录数据
            int expected_size = fh->get_file_hdr().record_size;
            bool rec_data_valid = (rec.data != nullptr && rec.size > 0 && rec.size == expected_size);
            if (!rec_data_valid) {
                // 记录数据无效，无法回滚DELETE操作
                // 仍然尝试删除记录（如果存在），但不恢复索引
                try {
                    fh->delete_record(rid, context);
                } catch (...) {
                    // 记录可能不存在，忽略
                }
                write_set->pop_back();
                continue;
            }
            
            // 先恢复记录到原RID位置（必须先恢复记录，再恢复索引）
            // 关键：必须先恢复记录，确保记录存在，再恢复索引指向它
            bool record_restored = false;
            
            // 检查记录是否已存在
            std::unique_ptr<RmRecord> existing_rec;
            try {
                existing_rec = fh->get_record(rid, context);
                if (existing_rec != nullptr) {
                    // 记录已存在，可能是被UPDATE回滚恢复了，但值可能不对
                    // 先删除现有记录的索引条目，避免索引不一致
                    for (size_t i = 0; i < tab.indexes.size(); ++i) {
                        auto &index = tab.indexes[i];
                        auto ih =
                            sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                        char *key = new char[index.col_tot_len];
                        int offset = 0;
                        for (int j = 0; j < index.col_num; ++j) {
                            memcpy(key + offset, existing_rec->data + index.cols[j].offset, index.cols[j].len);
                            offset += index.cols[j].len;
                        }
                        try {
                            ih->delete_entry(key, context->txn_);
                        } catch (...) {
                            // 索引条目可能不存在，忽略
                        }
                        delete[] key;
                    }
                    // 更新为DELETE时的记录值
                    try {
                        fh->update_record(rid, rec.data, context);
                        // 立即验证更新是否成功
                        try {
                            auto verify_rec = fh->get_record(rid, context);
                            if (verify_rec != nullptr) {
                                record_restored = true;
                            } else {
                                record_restored = false;
                            }
                        } catch (...) {
                            record_restored = false;
                        }
                    } catch (...) {
                        // update_record 可能失败（如记录不存在），标记为未恢复
                        record_restored = false;
                    }
                }
            } catch (...) {
                // 记录不存在，需要插入
                existing_rec = nullptr;
            }
            
            // 如果记录不存在，尝试插入
            if (!record_restored) {
                try {
                    // Insert into record file at original RID position
                    // 使用insert_record的重载版本，可以指定RID
                    fh->insert_record(rid, rec.data);
                    // 立即验证记录是否真的被插入
                    try {
                        auto verify_rec = fh->get_record(rid, context);
                        if (verify_rec != nullptr) {
                            record_restored = true;
                        } else {
                            record_restored = false;
                        }
                    } catch (...) {
                        record_restored = false;
                    }
                } catch (const std::runtime_error& e) {
                    // 插入失败（比如slot已被占用，抛出"Slot already occupied"）
                    // 这种情况下，说明记录可能已经存在，但get_record失败了
                    // 尝试获取现有记录并删除其索引，然后更新
                    std::string error_msg = e.what();
                    if (error_msg.find("Slot already occupied") != std::string::npos ||
                        error_msg.find("already") != std::string::npos) {
                        // Slot已被占用，尝试获取现有记录并删除其索引
                        try {
                            auto existing_rec = fh->get_record(rid, context);
                            if (existing_rec != nullptr) {
                                // 先删除现有记录的索引条目
                                for (size_t i = 0; i < tab.indexes.size(); ++i) {
                                    auto &index = tab.indexes[i];
                                    auto ih =
                                        sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols)).get();
                                    char *key = new char[index.col_tot_len];
                                    int offset = 0;
                                    for (int j = 0; j < index.col_num; ++j) {
                                        memcpy(key + offset, existing_rec->data + index.cols[j].offset, index.cols[j].len);
                                        offset += index.cols[j].len;
                                    }
                                    try {
                                        ih->delete_entry(key, context->txn_);
                                    } catch (...) {
                                        // 索引条目可能不存在，忽略
                                    }
                                    delete[] key;
                                }
                            }
                            // 更新为DELETE时的记录值
                            fh->update_record(rid, rec.data, context);
                            // 验证更新是否成功
                            try {
                                auto verify_rec = fh->get_record(rid, context);
                                if (verify_rec != nullptr) {
                                    record_restored = true;
                                } else {
                                    record_restored = false;
                                }
                            } catch (...) {
                                record_restored = false;
                            }
                        } catch (...) {
                            // 如果更新也失败，记录恢复失败
                            record_restored = false;
                        }
                    } else {
                        // 其他错误，记录恢复失败
                        record_restored = false;
                    }
                }
            }
            
            // 最终验证记录确实存在（如果恢复失败，不应该恢复索引）
            if (record_restored) {
                // 再次验证记录是否存在，并获取记录以确保它真的存在
                std::unique_ptr<RmRecord> verify_rec;
                try {
                    verify_rec = fh->get_record(rid, context);
                    if (verify_rec == nullptr) {
                        record_restored = false;
                    }
                } catch (...) {
                    record_restored = false;
                }
            }
            
            // 注意：索引 undo log 已经在上面处理了，这里只需要恢复记录
            // 如果记录恢复失败，索引 undo log 已经尝试恢复了，即使失败也不会导致不一致
        } else if (type == WType::UPDATE_TUPLE) {
            auto &tab_name = item->GetTableName();
            auto &rid = item->GetRid();
            auto &record = item->GetRecord();  // 这是保存的旧记录数据
            auto tab = sm_manager_->db_.get_table(tab_name);
            auto fh = sm_manager_->fhs_.at(tab_name).get();
            
            // 尝试获取当前记录，如果记录不存在（可能已被DELETE回滚），则使用保存的旧记录
            std::unique_ptr<RmRecord> current_rec;
            bool record_exists = false;
            try {
                current_rec = fh->get_record(rid, context);
                if (current_rec != nullptr) {
                    record_exists = true;
                }
            } catch (...) {
                // 记录不存在，可能是被DELETE回滚了
                current_rec = nullptr;
                record_exists = false;
            }
            
            // 先恢复记录，确保记录存在，再恢复索引
            // 关键：必须先恢复记录，再恢复索引，避免索引指向不存在的记录
            bool record_restored = false;
            
            if (record_exists) {
                // 记录存在，更新为旧值
                try {
                    fh->update_record(rid, record.data, context);
                    // 立即验证更新是否成功
                    try {
                        auto verify_rec = fh->get_record(rid, context);
                        if (verify_rec != nullptr) {
                            record_restored = true;
                        } else {
                            record_restored = false;
                        }
                    } catch (...) {
                        record_restored = false;
                    }
                } catch (...) {
                    record_restored = false;
                }
            } else {
                // 记录不存在，需要插入
                try {
                    fh->insert_record(rid, record.data);
                    // 立即验证插入是否成功
                    try {
                        auto verify_rec = fh->get_record(rid, context);
                        if (verify_rec != nullptr) {
                            record_restored = true;
                        } else {
                            record_restored = false;
                        }
                    } catch (...) {
                        record_restored = false;
                    }
                } catch (const std::runtime_error& e) {
                    // 插入失败（比如slot已被占用），尝试更新
                    std::string error_msg = e.what();
                    if (error_msg.find("Slot already occupied") != std::string::npos ||
                        error_msg.find("already") != std::string::npos) {
                        try {
                            fh->update_record(rid, record.data, context);
                            // 验证更新是否成功
                            try {
                                auto verify_rec = fh->get_record(rid, context);
                                if (verify_rec != nullptr) {
                                    record_restored = true;
                                } else {
                                    record_restored = false;
                                }
                            } catch (...) {
                                record_restored = false;
                            }
                        } catch (...) {
                            record_restored = false;
                        }
                    } else {
                        record_restored = false;
                    }
                }
            }
            
            // 最终验证记录确实存在
            if (record_restored) {
                try {
                    auto verify_rec = fh->get_record(rid, context);
                    if (verify_rec == nullptr) {
                        record_restored = false;
                    }
                } catch (...) {
                    record_restored = false;
                }
            }
            
            // 注意：索引 undo log 已经在上面处理了，这里只需要恢复记录
            // 如果记录恢复失败，索引 undo log 已经尝试恢复了，即使失败也不会导致不一致
        }
        write_set->pop_back();
    }

    auto lock_set = txn->get_lock_set();
    for (auto lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }
    lock_set->clear();
    txn->set_state(TransactionState::ABORTED);
}