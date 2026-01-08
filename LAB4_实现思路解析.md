# Lab4 并发控制实验实现思路详细解析

## 一、实验总体概述

Lab4是RucBase的并发控制实验，主要目标是实现基于**两阶段封锁协议（2PL）**的并发控制机制，保证事务的**可串行化隔离级别**。实验分为三个部分：

1. **实验一：事务管理器实现（40分）**
2. **实验二：并发控制实验（60分）**
3. **附加实验：索引相关和幻读处理（40分）**

---

## 二、实验一：事务管理器实现

### 2.1 核心任务
实现`TransactionManager`类的三个核心方法：
- `begin()`: 事务开始
- `commit()`: 事务提交
- `abort()`: 事务回滚

### 2.2 实现思路

#### 2.2.1 `begin()` 方法
**功能**：启动一个新事务

**实现步骤**：
```cpp
Transaction *TransactionManager::begin(Transaction *txn, LogManager *log_manager) {
    std::scoped_lock lock(latch_);  // 保护txn_map的并发访问
    
    if (txn == nullptr) {
        // 创建新事务，分配唯一的事务ID
        txn = new Transaction(next_txn_id_++);
    }
    
    // 将事务加入到全局事务映射表中
    txn_map[txn->get_transaction_id()] = txn;
    
    return txn;
}
```

**关键点**：
- 使用互斥锁保护`txn_map`，确保多线程安全
- 如果传入的`txn`为空，创建新事务并分配递增的事务ID
- 将事务注册到全局事务表中，便于后续查找和管理

#### 2.2.2 `commit()` 方法
**功能**：提交事务，持久化所有写操作

**实现步骤**：
```cpp
void TransactionManager::commit(Transaction *txn, LogManager *log_manager) {
    std::scoped_lock lock(latch_);
    
    // 1. 清空写操作集合（写操作已经持久化）
    auto write_set = txn->get_write_set();
    write_set->clear();
    
    // 2. 释放所有锁（2PL第二阶段：收缩阶段）
    auto lock_set = txn->get_lock_set();
    for (auto lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }
    lock_set->clear();
    
    // 3. 更新事务状态为已提交
    txn->set_state(TransactionState::COMMITTED);
}
```

**关键点**：
- 清空写操作集合，因为写操作已经持久化到磁盘
- **两阶段封锁协议**：提交时释放所有锁（收缩阶段）
- 更新事务状态，标记事务已成功提交

#### 2.2.3 `abort()` 方法
**功能**：回滚事务，撤销所有写操作

**实现步骤**：
```cpp
void TransactionManager::abort(Transaction *txn, LogManager *log_manager) {
    std::scoped_lock lock(latch_);
    
    auto write_set = txn->get_write_set();
    Context *context = new Context(lock_manager_, log_manager, txn);
    
    // 1. 反向执行所有写操作，实现回滚
    while (!write_set->empty()) {
        auto &item = write_set->back();
        WType type = item->GetWriteType();
        
        if (type == WType::INSERT_TUPLE) {
            // 插入操作的回滚：删除插入的记录
            // 需要同时删除索引和记录文件中的记录
        } 
        else if (type == WType::DELETE_TUPLE) {
            // 删除操作的回滚：重新插入被删除的记录
            // 需要同时恢复索引和记录文件
        } 
        else if (type == WType::UPDATE_TUPLE) {
            // 更新操作的回滚：恢复记录到原来的值
            // 需要更新索引（删除新索引，插入旧索引）
        }
        
        write_set->pop_back();
    }
    
    // 2. 释放所有锁
    auto lock_set = txn->get_lock_set();
    for (auto lock : *lock_set) {
        lock_manager_->unlock(txn, lock);
    }
    lock_set->clear();
    
    // 3. 更新事务状态为已中止
    txn->set_state(TransactionState::ABORTED);
}
```

**关键点**：
- **反向执行**：按照写操作的逆序执行回滚
- **完整回滚**：不仅回滚记录文件，还要回滚索引的变更
- 回滚DELETE操作时，**必须插入到原位置**，否则：
  - 索引指向的位置会改变
  - RID的引用会失效
  - 可能导致数据不一致

---

## 三、实验二：并发控制实验

### 3.1 核心任务
实现`LockManager`类，提供多粒度锁机制，并集成到执行器中。

### 3.2 锁的类型和粒度

#### 3.2.1 锁的类型
1. **共享锁（S锁）**：读锁，多个事务可以同时持有
2. **排他锁（X锁）**：写锁，独占访问
3. **意向共享锁（IS锁）**：表级锁，表示事务将在某些行上申请S锁
4. **意向排他锁（IX锁）**：表级锁，表示事务将在某些行上申请X锁

#### 3.2.2 锁的粒度
- **表级锁**：锁定整个表
- **行级锁**：锁定表中的特定行
- **间隙锁（GAP锁）**：锁定索引键值之间的间隙，用于防止幻读

### 3.3 锁相容矩阵

|        | IS  | IX  | S   | X   | SIX |
|--------|-----|-----|-----|-----|-----|
| **IS** | ✓   | ✓   | ✓   | ✗   | ✓   |
| **IX** | ✓   | ✓   | ✗   | ✗   | ✗   |
| **S**  | ✓   | ✗   | ✓   | ✗   | ✗   |
| **X**  | ✗   | ✗   | ✗   | ✗   | ✗   |
| **SIX**| ✓   | ✗   | ✗   | ✗   | ✗   |

**解读**：
- IS锁和IX锁之间相容
- S锁和IS锁相容，但与IX锁不相容
- X锁与其他所有锁都不相容
- 相容意味着可以同时授予，不相容则不能同时授予

### 3.4 锁管理器实现

#### 3.4.1 核心数据结构

```cpp
class LockManager {
private:
    std::mutex latch_;  // 保护lock_table_的互斥锁
    std::unordered_map<LockDataId, LockRequestQueue> lock_table_;  // 全局锁表
};

class LockRequestQueue {
    std::list<LockRequest> request_queue_;  // 锁请求队列
    GroupLockMode group_lock_mode_;  // 队列中排他性最强的锁类型
    int shared_lock_num_;  // 共享锁数量
    int IX_lock_num_;  // IX锁数量
};
```

#### 3.4.2 行级锁加锁实现

**共享锁（S锁）申请**：
```cpp
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // 1. 检查事务状态（不能是COMMITTED、ABORTED、SHRINKING状态）
    if (!check_lock(txn)) return false;
    
    // 2. 构造锁ID
    LockDataId lockDataId(tab_fd, rid, LockDataType::RECORD);
    
    // 3. 检查是否已经持有锁
    for (auto& lockRequest : lockRequests) {
        if (lockRequest.txn_id_ == txn->get_transaction_id()) {
            if (lockRequest.lock_mode_ == LockMode::SHARED || 
                lockRequest.lock_mode_ == LockMode::EXLUCSIVE) {
                return true;  // 已持有兼容锁
            }
        }
    }
    
    // 4. 检查锁相容性（no-wait策略）
    if (lockRequestQueue.group_lock_mode_ == GroupLockMode::X ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::IX ||
        lockRequestQueue.group_lock_mode_ == GroupLockMode::SIX) {
        // 不相容，直接失败（死锁预防）
        throw TransactionAbortException(txn->get_transaction_id(), 
                                       AbortReason::DEADLOCK_PREVENTION);
    }
    
    // 5. 授予锁
    lockRequestQueue.request_queue_.emplace_back(txn->get_transaction_id(), 
                                                 LockMode::SHARED);
    ++lockRequestQueue.shared_lock_num_;
    lockRequestQueue.request_queue_.back().granted_ = true;
    txn->get_lock_set()->emplace(lockDataId);
    
    return true;
}
```

**排他锁（X锁）申请**：
```cpp
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    // 类似S锁，但有以下区别：
    
    // 1. 检查是否可以锁升级（从S锁升级到X锁）
    if (已持有S锁 && 只有当前事务持有S锁) {
        // 可以升级
        lockRequest.lock_mode_ = LockMode::EXLUCSIVE;
        --lockRequestQueue.shared_lock_num_;
        return true;
    }
    
    // 2. 检查锁相容性
    if (有其他任何锁) {
        // X锁与其他所有锁都不相容
        throw TransactionAbortException(...);
    }
    
    // 3. 授予X锁
    // ...
}
```

**关键点**：
- **No-Wait策略**：如果锁冲突，立即中止事务，而不是等待（死锁预防）
- **锁升级**：同一事务可以从S锁升级到X锁（如果只有当前事务持有S锁）
- **事务状态检查**：在SHRINKING阶段不能申请新锁（2PL约束）

#### 3.4.3 表级锁和意向锁实现

**意向锁的作用**：
- 在行级加锁之前，先申请表级意向锁
- 提高并发度：多个事务可以在同一表的不同行上持有锁
- 表级操作（如DROP TABLE）可以快速检查是否有行级锁

**IS锁申请**：
```cpp
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查是否已持有IS或更高级锁
    // 2. 检查与X锁的相容性
    // 3. 授予IS锁
}
```

**IX锁申请**：
```cpp
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    // 1. 检查是否已持有IX或更高级锁
    // 2. 检查是否可以升级（IS -> IX, S -> SIX）
    // 3. 检查与S锁和X锁的相容性
    // 4. 授予IX锁
}
```

#### 3.4.4 解锁实现

```cpp
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    
    // 1. 检查事务状态
    if (txn->get_state() == TransactionState::COMMITTED || 
        txn->get_state() == TransactionState::ABORTED) {
        return false;
    }
    
    // 2. 进入收缩阶段（2PL第二阶段）
    if (txn->get_state() == TransactionState::GROWING) {
        txn->set_state(TransactionState::SHRINKING);
    }
    
    // 3. 从锁表中移除该锁请求
    // 4. 更新队列的锁模式
    // 5. 从事务的锁集合中移除
    
    return true;
}
```

**关键点**：
- 解锁时进入**收缩阶段（SHRINKING）**
- 收缩阶段不能再申请新锁（2PL的严格性保证）

### 3.5 两阶段封锁协议的集成

#### 3.5.1 在读操作中加锁

**SeqScan执行器**：
```cpp
void SeqScanExecutor::beginTuple() {
    // 1. 申请IS意向锁（表级）
    if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
        int tab_fd = fh_->GetFd();
        if (!context_->lock_mgr_->lock_IS_on_table(context_->txn_, tab_fd)) {
            throw std::runtime_error("Failed to acquire IS lock on table");
        }
    }
    
    // 2. 开始扫描...
}

// 在get_record时申请行级S锁
auto rec = fh_->get_record(rid_, context_);  // 内部会申请S锁
```

**RmFileHandle::get_record()**：
```cpp
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) {
    // 申请行级共享锁
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        if (!context->lock_mgr_->lock_shared_on_record(context->txn_, rid, fd_)) {
            throw std::runtime_error("Failed to acquire shared lock on record");
        }
    }
    
    // 读取记录...
}
```

#### 3.5.2 在写操作中加锁

**Insert执行器**：
```cpp
std::unique_ptr<RmRecord> InsertExecutor::Next() {
    // 1. 申请IX意向锁（表级）
    if (context_ != nullptr && context_->txn_ != nullptr && context_->lock_mgr_ != nullptr) {
        int tab_fd = fh_->GetFd();
        if (!context_->lock_mgr_->lock_IX_on_table(context_->txn_, tab_fd)) {
            throw std::runtime_error("Failed to acquire IX lock on table");
        }
    }
    
    // 2. 插入记录（内部会申请行级X锁）
    rid_ = fh_->insert_record(rec.data, context_);
    
    // 3. 对于索引上的插入，可能需要间隙锁（防止幻读）
    // ...
}
```

**RmFileHandle::delete_record()**：
```cpp
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 申请行级排他锁
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        if (!context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_)) {
            throw std::runtime_error("Failed to acquire exclusive lock on record");
        }
    }
    
    // 删除记录...
}
```

**RmFileHandle::update_record()**：
```cpp
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    // 申请行级排他锁
    if (context != nullptr && context->txn_ != nullptr && context->lock_mgr_ != nullptr) {
        if (!context->lock_mgr_->lock_exclusive_on_record(context->txn_, rid, fd_)) {
            throw std::runtime_error("Failed to acquire exclusive lock on record");
        }
    }
    
    // 更新记录...
}
```

#### 3.5.3 表级操作中的锁申请

**CREATE TABLE、DROP TABLE**：
- 申请表级X锁（独占整个表）

**CREATE INDEX、DROP INDEX**：
- 申请表级IX锁（对表的索引进行操作）

### 3.6 防止的数据异常

通过2PL协议和锁机制，可以防止以下数据异常：

1. **脏读（Dirty Read）**
   - 写事务持有X锁，读事务无法获取S锁
   - 只有提交后才释放X锁

2. **脏写（Dirty Write）**
   - X锁互斥，两个写事务不能同时对同一行加X锁

3. **不可重复读（Unrepeatable Read）**
   - 读事务持有S锁，直到事务结束才释放
   - 写事务无法获取X锁

4. **丢失更新（Lost Update）**
   - 写事务必须持有X锁才能修改
   - 即使两个事务都先读取（S锁），升级到X锁时也会冲突

---

## 四、附加实验：索引和幻读处理

### 4.1 索引相关的事务处理

#### 4.1.1 索引的回滚

在`abort()`方法中，需要同时回滚索引：
- INSERT回滚：删除索引项
- DELETE回滚：重新插入索引项
- UPDATE回滚：先删除新索引项，再插入旧索引项

**关键点**：
- 索引和记录文件必须保持一致性
- 回滚顺序很重要：先回滚索引，再回滚记录

#### 4.1.2 索引扫描的支持

需要实现`IndexScanExecutor`，支持基于索引的扫描：
- 利用索引加速查询
- 索引扫描也需要加锁（对扫描的键值范围加锁）

### 4.2 幻读（Phantom Read）的解决

#### 4.2.1 问题描述

**幻读场景**：
```
事务T1: SELECT * FROM t WHERE id > 10;  -- 返回5条记录
事务T2: INSERT INTO t VALUES (15, ...);  -- 插入一条满足条件的记录
事务T1: SELECT * FROM t WHERE id > 10;  -- 返回6条记录（幻影）
```

#### 4.2.2 解决方案：间隙锁（GAP Lock）

**间隙锁的实现**：
```cpp
bool LockManager::lock_shared_on_gap(Transaction* txn, int tab_fd, 
                                     int left_key, int right_key) {
    // 锁定索引键值区间 [left_key, right_key)
    // 防止其他事务在该区间插入数据
}

bool LockManager::lock_exclusive_on_gap(Transaction* txn, int tab_fd,
                                        int left_key, int right_key) {
    // 排他间隙锁，用于INSERT操作
    // 防止其他事务在该区间进行任何操作
}
```

**在查询中使用间隙锁**：
```cpp
// 索引扫描时，对扫描的键值范围申请共享间隙锁
if (使用索引扫描) {
    context->lock_mgr_->lock_shared_on_gap(context->txn_, tab_fd, 
                                           min_key, max_key);
}
```

**在插入中使用间隙锁**：
```cpp
// InsertExecutor::Next()中
if (有索引 && 是INT类型单列索引) {
    int insert_key = *reinterpret_cast<int*>(key);
    // 对插入的key所在的间隙申请排他间隙锁
    if (!context_->lock_mgr_->lock_exclusive_on_gap(
            context_->txn_, tab_fd, insert_key, insert_key)) {
        throw std::runtime_error("Failed to acquire exclusive gap lock");
    }
}
```

**间隙锁的作用**：
- 共享间隙锁：防止其他事务在该区间插入数据
- 排他间隙锁：确保插入操作不会与其他事务的查询冲突

#### 4.2.3 间隙锁与表锁的权衡

- **表锁方案**：简单但并发度低（所有操作串行化）
- **间隙锁方案**：复杂但并发度高（只锁定相关区间）

实验要求使用间隙锁方案以获得更高的并发度。

---

## 五、实现的关键技术点总结

### 5.1 两阶段封锁协议（2PL）

**两个阶段**：
1. **增长阶段（Growing Phase）**：只能申请锁，不能释放锁
2. **收缩阶段（Shrinking Phase）**：只能释放锁，不能申请新锁

**实现要点**：
- 事务状态机：DEFAULT -> GROWING -> SHRINKING -> COMMITTED/ABORTED
- 在`unlock()`时进入收缩阶段
- 在收缩阶段申请新锁会抛出异常

### 5.2 死锁预防：No-Wait策略

**策略**：
- 如果申请的锁与其他事务的锁冲突，立即中止当前事务
- 不等待锁释放，避免死锁检测的复杂性

**实现**：
```cpp
if (锁冲突) {
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK_PREVENTION);
}
```

### 5.3 锁升级

**场景**：
- 同一事务先读后写同一行：S锁 -> X锁
- 表级锁升级：IS -> IX, S -> SIX

**条件**：
- 只有当前事务持有锁时才能升级
- 需要检查`shared_lock_num_`等计数器

### 5.4 多粒度锁

**层次结构**：
- 表级锁（TABLE）
- 行级锁（RECORD）
- 间隙锁（GAP）

**意向锁的作用**：
- 在行级加锁前申请表级意向锁
- 表级操作可以快速判断是否有行级锁

### 5.5 事务状态管理

**状态转换**：
```
DEFAULT -> GROWING -> SHRINKING -> COMMITTED/ABORTED
  |         |           |              |
  |         |           |              +-- 事务结束
  |         |           +-- 首次解锁
  |         +-- 首次加锁
  +-- 事务创建
```

### 5.6 回滚机制

**WriteRecord的作用**：
- 记录所有写操作（INSERT、DELETE、UPDATE）
- 保存回滚所需的信息（RID、旧值等）

**回滚顺序**：
- 按照写操作的**逆序**执行回滚
- 同时回滚索引和记录文件

---

## 六、测试要点

### 6.1 事务管理器测试
- `commit_test`：测试事务提交
- `abort_test`：测试事务回滚

### 6.2 并发控制测试
- `concurrency_read_test`：并发读取
- `dirty_write_test`：防止脏写
- `dirty_read_test`：防止脏读
- `lost_update_test`：防止丢失更新
- `unrepeatable_read_test`：防止不可重复读

### 6.3 附加测试
- `commit_index_test`：索引相关的事务提交
- `abort_index_test`：索引相关的事务回滚
- `phantom_read_test`：防止幻读（间隙锁）

---

## 七、常见问题和注意事项

1. **锁的申请顺序**：先申请表级意向锁，再申请行级锁
2. **回滚顺序**：按照写操作的逆序回滚
3. **索引一致性**：回滚时必须同时回滚索引
4. **事务状态检查**：加锁前检查事务状态
5. **线程安全**：使用互斥锁保护共享数据结构（`txn_map`、`lock_table_`）
6. **RID的使用**：回滚DELETE时必须插入到原RID位置
7. **间隙锁的范围**：需要正确计算查询的键值范围

---

## 八、实现流程图

### 8.1 事务执行流程
```
BEGIN
  |
  v
申请IS/IX锁（表级）
  |
  v
执行操作
  |
  v
申请S/X锁（行级/索引）
  |
  v
执行读写操作
  |
  v
COMMIT/ABORT
  |
  v
释放所有锁
  |
  v
结束
```

### 8.2 加锁流程
```
检查事务状态
  |
  v
检查是否已持有锁
  |
  v
检查锁相容性
  |
  v
授予锁 / 抛出异常（no-wait）
```

### 8.3 回滚流程
```
获取write_set
  |
  v
逆序遍历write_set
  |
  v
根据操作类型执行反向操作
  |
  v
同时回滚索引和记录
  |
  v
清空write_set和lock_set
```

---

这份文档详细解析了Lab4的实现思路，涵盖了事务管理、锁管理、并发控制和幻读处理等核心内容。希望对你理解Lab4的实现有所帮助！


