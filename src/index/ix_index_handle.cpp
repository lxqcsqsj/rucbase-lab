/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_index_handle.h"

#include "ix_scan.h"

/**
 * @brief 在当前node中查找第一个>=target的key_idx
 *
 * @return key_idx，范围为[0,num_key)，如果返回的key_idx=num_key，则表示target大于最后一个key
 * @note 返回key index（同时也是rid index），作为slot no
 */
int IxNodeHandle::lower_bound(const char *target) const {
    int left = 0;
    int right = page_hdr->num_key;
    //采用二分查找
    while (left < right) {
        int mid = (left + right) / 2;
        char *mid_key = get_key(mid);
        //使用ix_compare()函数进行比较
        int cmp = ix_compare(mid_key, target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    //返回当前节点中第一个大于等于target的key的位置
    return left;
}

/**
 * @brief 在当前node中查找第一个>target的key_idx
 *
 * @return key_idx，范围为[1,num_key)，如果返回的key_idx=num_key，则表示target大于等于最后一个key
 * @note 注意此处的范围从1开始
 */
int IxNodeHandle::upper_bound(const char *target) const {
    int left = 0;
    int right = page_hdr->num_key;
    //二分查找
    while (left < right) {
        int mid = (left + right) / 2;
        char *mid_key = get_key(mid);
        int cmp = ix_compare(mid_key, target, file_hdr->col_types_, file_hdr->col_lens_);
        if (cmp <= 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    return left;
}

/**
 * @brief 用于叶子结点根据key来查找该结点中的键值对
 * 值value作为传出参数，函数返回是否查找成功
 *
 * @param key 目标key
 * @param[out] value 传出参数，目标key对应的Rid
 * @return 目标key是否存在
 */
bool IxNodeHandle::leaf_lookup(const char *key, Rid **value) {
    //在叶子节点获取目标key所在位置
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key) {
        char *found_key = get_key(pos);
        //判断key是否存在且匹配
        if (ix_compare(found_key, key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
            //如果存在，获取key对应的Rid，并赋值传出参数value
            *value = get_rid(pos);
            return true;
        }
    }
    return false;
}

/**
 * 用于内部结点（非叶子节点）查找目标key所在的孩子结点（子树）
 * @param key 目标key
 * @return page_id_t 目标key所在的孩子节点（子树）的存储页面编号
 */
page_id_t IxNodeHandle::internal_lookup(const char *key) {
    //查找当前非叶子节点中目标key所在孩子节点（子树）的位置
    int pos = upper_bound(key);
    if (pos == 0) {
        return value_at(0);
    }
    //返回页面编号
    return value_at(pos - 1);
}

/**
 * @brief 在指定位置插入n个连续的键值对
 * 将key的前n位插入到原来keys中的pos位置；将rid的前n位插入到原来rids中的pos位置
 *
 * @param pos 要插入键值对的位置
 * @param (key, rid) 连续键值对的起始地址，也就是第一个键值对，可以通过(key, rid)来获取n个键值对
 * @param n 键值对数量
 * @note [0,pos)           [pos,num_key)
 *                            key_slot
 *                            /      \
 *                           /        \
 *       [0,pos)     [pos,pos+n)   [pos+n,num_key+n)
 *                      key           key_slot
 */
void IxNodeHandle::insert_pairs(int pos, const char *key, const Rid *rid, int n) {
    //判断pos合法性
    assert(pos >= 0 && pos <= page_hdr->num_key);
    if (n <= 0) {
        return;
    }
    //需要移动的数据量
    int key_size = file_hdr->col_tot_len_;
    int move_key_bytes = (page_hdr->num_key - pos) * key_size;
    //移动key数组，空出n个位置
    if (move_key_bytes > 0) {
        memmove(keys + (pos + n) * key_size, keys + pos * key_size, move_key_bytes);
    }
    //移动Rid数组
    int move_rid_bytes = (page_hdr->num_key - pos) * sizeof(Rid);
    if (move_rid_bytes > 0) {
        memmove(&rids[pos + n], &rids[pos], move_rid_bytes);
    }
    //插入新数据
    memcpy(keys + pos * key_size, key, n * key_size);
    memcpy(&rids[pos], rid, n * sizeof(Rid));
    //更新节点的键数量
    page_hdr->num_key += n;
}

/**
 * @brief 用于在结点中插入单个键值对。
 * 函数返回插入后的键值对数量
 *
 * @param (key, value) 要插入的键值对
 * @return int 键值对数量
 */
int IxNodeHandle::insert(const char *key, const Rid &value) {
    //查找要插入的键值对应该插入到当前节点的哪个位置
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key) {
        char *exist_key = get_key(pos);
        //如果key重复则不插入
        if (ix_compare(exist_key, key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
            return page_hdr->num_key;
        }
    }
    //不重复则插入
    insert_pairs(pos, key, &value, 1);
    //返回完成插入操作之后的键值对数量
    return page_hdr->num_key;
}

/**
 * @brief 用于在结点中的指定位置删除单个键值对
 *
 * @param pos 要删除键值对的位置
 */
void IxNodeHandle::erase_pair(int pos) {
    assert(pos >= 0 && pos < page_hdr->num_key);
    //计算需要移动的剩余键值对数
    int key_size = file_hdr->col_tot_len_;
    int move_key_bytes = (page_hdr->num_key - pos - 1) * key_size;
    //移动key
    if (move_key_bytes > 0) {
        memmove(keys + pos * key_size, keys + (pos + 1) * key_size, move_key_bytes);
    }
    //移动rid
    int move_rid_bytes = (page_hdr->num_key - pos - 1) * sizeof(Rid);
    if (move_rid_bytes > 0) {
        memmove(&rids[pos], &rids[pos + 1], move_rid_bytes);
    }
    //更新键值对数
    page_hdr->num_key--;
}

/**
 * @brief 用于在结点中删除指定key的键值对。函数返回删除后的键值对数量
 *
 * @param key 要删除的键值对key值
 * @return 完成删除操作后的键值对数量
 */
int IxNodeHandle::remove(const char *key) {
    //查找位置
    int pos = lower_bound(key);
    if (pos < page_hdr->num_key) {
        char *exist_key = get_key(pos);
        //如果存在，删除
        if (ix_compare(exist_key, key, file_hdr->col_types_, file_hdr->col_lens_) == 0) {
            erase_pair(pos);
        }
    }
    //返回键值对数量
    return page_hdr->num_key;
}

IxIndexHandle::IxIndexHandle(DiskManager *disk_manager, BufferPoolManager *buffer_pool_manager, int fd)
    : disk_manager_(disk_manager), buffer_pool_manager_(buffer_pool_manager), fd_(fd) {
    // init file_hdr_
    char *buf = new char[PAGE_SIZE];
    disk_manager_->read_page(fd, IX_FILE_HDR_PAGE, buf, PAGE_SIZE);
    file_hdr_ = new IxFileHdr();
    file_hdr_->deserialize(buf);
    delete[] buf;

    // disk_manager管理的fd对应的文件中，设置从file_hdr_->num_pages开始分配page_no
    int now_page_no = disk_manager_->get_fd2pageno(fd);
    disk_manager_->set_fd2pageno(fd, now_page_no + 1);
}

/**
 * @brief 用于查找指定键所在的叶子结点
 * @param key 要查找的目标key值
 * @param operation 查找到目标键值对后要进行的操作类型
 * @param transaction 事务参数，如果不需要则默认传入nullptr
 * @return [leaf node] and [root_is_latched] 返回目标叶子结点以及根结点是否加锁
 * @note need to Unlatch and unpin the leaf node outside!
 * 注意：用了FindLeafPage之后一定要unlatch叶结点，否则下次latch该结点会堵塞！
 */
std::pair<IxNodeHandle *, bool> IxIndexHandle::find_leaf_page(const char *key, Operation operation,
                                                            Transaction *transaction, bool find_first) {
    //B+树非空
    if (file_hdr_->root_page_ == IX_NO_PAGE) {
        return {nullptr, false};
    }

    //锁住写操作，避免并发写冲突
    bool root_is_latched = false;
    if (operation != Operation::FIND) {
        root_latch_.lock();
        root_is_latched = true;
    }

    //获取根节点
    IxNodeHandle *node = fetch_node(file_hdr_->root_page_);
    //不断向下查找目标key
    while (!node->is_leaf_page()) {
        page_id_t child_no;
        if (find_first) {
            child_no = node->value_at(0);   //第一个子节点
        } else {
            child_no = node->internal_lookup(key);  //key所在的子节点
        }
        IxNodeHandle *child = fetch_node(child_no);
        //清理父节点
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        delete node;
        node = child;
    }
    //返回叶子节点+锁状态
    return {node, root_is_latched};
}

/**
 * @brief 用于查找指定键在叶子结点中的对应的值result
 *
 * @param key 查找的目标key值
 * @param result 用于存放结果的容器
 * @param transaction 事务指针
 * @return bool 返回目标键值对是否存在
 */
bool IxIndexHandle::get_value(const char *key, std::vector<Rid> *result, Transaction *transaction) {
    //获取目标所在的叶子节点
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, transaction);
    if (leaf == nullptr) {
        return false;
    }

    //在叶子节点查找目标key值的位置，并读取key对应的rid
    Rid *value = nullptr;
    bool found = leaf->leaf_lookup(key, &value);
    //把rid存入result参数中
    if (found && result != nullptr) {
        result->push_back(*value);
    }

    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    delete leaf;
    //使用find_leaf_page后解锁
    if (root_is_latched) {
        root_latch_.unlock();
    }
    return found;
}

/**
 * @brief  将传入的一个node拆分(Split)成两个结点，在node的右边生成一个新结点new node
 * @param node 需要拆分的结点
 * @return 拆分得到的new_node
 * @note need to unpin the new node outside
 * 注意：本函数执行完毕后，原node和new node都需要在函数外面进行unpin
 */
IxNodeHandle *IxIndexHandle::split(IxNodeHandle *node) {
    //在node的右边生成一个新结点new node，初始化新节点的page_hdr内容
    IxNodeHandle *new_node = create_node();
    new_node->page_hdr->is_leaf = node->is_leaf_page();
    new_node->set_parent_page_no(node->get_parent_page_no());

    //将原节点的键值对平均分配
    int total = node->get_size();
    int move_count = total / 2;
    int start = total - move_count;

    //右半部分分裂为新的右兄弟结点，为新节点分配键值对，更新旧节点的键值对数记录
    new_node->insert_pairs(0, node->get_key(start), node->get_rid(start), move_count);
    node->set_size(start);

    //如果新的右兄弟结点是叶子结点
    if (node->is_leaf_page()) {
        //更新新旧节点的prev_leaf和next_leaf指针
        new_node->set_prev_leaf(node->get_page_no());
        new_node->set_next_leaf(node->get_next_leaf());
        node->set_next_leaf(new_node->get_page_no());

        // 更新后继节点的前驱指针
        if (new_node->get_next_leaf() != IX_NO_PAGE) {
            IxNodeHandle *next = fetch_node(new_node->get_next_leaf());
            next->set_prev_leaf(new_node->get_page_no());
            buffer_pool_manager_->unpin_page(next->get_page_id(), true);
            delete next;
        }

        // 更新文件头中的最后一个叶子指针
        if (file_hdr_->last_leaf_ == node->get_page_no()) {
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }
    } else {
        for (int i = 0; i < new_node->get_size(); i++) {
            //更新该结点的所有孩子结点的父节点信息(使用IxIndexHandle::maintain_child())
            maintain_child(new_node, i);
        }
    }

    return new_node;
}

/**
 * @brief Insert key & value pair into internal page after split
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>=maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 *
 * @param (old_node, new_node) 原结点为old_node，old_node被分裂之后产生了新的右兄弟结点new_node
 * @param key 要插入parent的key
 * @note 一个结点插入了键值对之后需要分裂，分裂后左半部分的键值对保留在原结点，在参数中称为old_node，
 * 右半部分的键值对分裂为新的右兄弟节点，在参数中称为new_node（参考Split函数来理解old_node和new_node）
 * @note 本函数执行完毕后，new node和old node都需要在函数外面进行unpin
 */
void IxIndexHandle::insert_into_parent(IxNodeHandle *old_node, const char *key, IxNodeHandle *new_node,
                                     Transaction *transaction) {
    //分裂前的结点（原结点, old_node）是否为根结点
    if (old_node->is_root_page()) {
        //如果为根结点需要分配新的root
        IxNodeHandle *new_root = create_node();
        new_root->page_hdr->is_leaf = false;
        new_root->set_parent_page_no(IX_NO_PAGE);

        new_root->insert_pair(0, old_node->get_key(0), {old_node->get_page_no(), -1});
        new_root->insert_pair(1, key, {new_node->get_page_no(), -1});

        old_node->set_parent_page_no(new_root->get_page_no());
        new_node->set_parent_page_no(new_root->get_page_no());

        update_root_page_no(new_root->get_page_no());
        // 如果是第一次创建索引（空树情况）
        if (file_hdr_->first_leaf_ == IX_NO_PAGE) {
            file_hdr_->first_leaf_ = old_node->get_page_no();
            file_hdr_->last_leaf_ = new_node->get_page_no();
        }

        buffer_pool_manager_->unpin_page(new_root->get_page_id(), true);
        delete new_root;
        return;
    }

    //获取原结点（old_node）的父亲结点
    IxNodeHandle *parent = fetch_node(old_node->get_parent_page_no());
    //获取key对应的rid
    int index = parent->find_child(old_node);
    //并将(key, rid)插入到父亲结点
    parent->insert_pair(index + 1, key, {new_node->get_page_no(), -1});
    new_node->set_parent_page_no(parent->get_page_no());

    //如果父亲结点仍需要继续分裂，则进行递归插入
    if (parent->get_size() >= parent->get_max_size()) {
        IxNodeHandle *new_parent = split(parent);
        char *push_up_key = new_parent->get_key(0);
        insert_into_parent(parent, push_up_key, new_parent, transaction);
        buffer_pool_manager_->unpin_page(new_parent->get_page_id(), true);
        delete new_parent;
    } else {
        maintain_parent(parent);
    }

    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    delete parent;
}

/**
 * @brief 将指定键值对插入到B+树中
 * @param (key, value) 要插入的键值对
 * @param transaction 事务指针
 * @return page_id_t 插入到的叶结点的page_no
 */
page_id_t IxIndexHandle::insert_entry(const char *key, const Rid &value, Transaction *transaction) {
    //查找key值应该插入到哪个叶子节点
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::INSERT, transaction);
    if (leaf == nullptr) {
        if (root_is_latched) {
            root_latch_.unlock();
        }
        return IX_NO_PAGE;
    }

    //在该叶子节点中插入键值对
    page_id_t leaf_page_no = leaf->get_page_no();
    int new_size = leaf->insert(key, value);

    //若当前叶子节点是最右叶子节点，则需要更新file_hdr_.last_leaf
    if (leaf_page_no == file_hdr_->last_leaf_) {
        file_hdr_->last_leaf_ = leaf_page_no;
    }

    //如果结点已满
    if (new_size >= leaf->get_max_size()) {
        //分裂结点
        IxNodeHandle *new_leaf = split(leaf);
        if (file_hdr_->last_leaf_ == leaf->get_page_no()) {
            file_hdr_->last_leaf_ = new_leaf->get_page_no();
        }
        char *split_key = new_leaf->get_key(0);
        //把新结点的相关信息插入父节点
        insert_into_parent(leaf, split_key, new_leaf, transaction);
        maintain_parent(leaf);
        buffer_pool_manager_->unpin_page(new_leaf->get_page_id(), true);
        delete new_leaf;
    } else {
        maintain_parent(leaf);
    }

    buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
    delete leaf;
    if (root_is_latched) {
        root_latch_.unlock();
    }
    return leaf_page_no;
}

/**
 * @brief 用于删除B+树中含有指定key的键值对
 * @param key 要删除的key值
 * @param transaction 事务指针
 */
bool IxIndexHandle::delete_entry(const char *key, Transaction *transaction) {
    //获取该键值对所在的叶子结点
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::DELETE, transaction);
    if (leaf == nullptr) {
        if (root_is_latched) {
            root_latch_.unlock();
        }
        return false;
    }

    //在该叶子结点中删除键值对
    int old_size = leaf->get_size();
    int new_size = leaf->remove(key);
    bool removed = (new_size < old_size);

    bool leaf_deleted = false;
    if (removed) {
        if (leaf->is_root_page()) {
            adjust_root(leaf);
        } else if (new_size < leaf->get_min_size()) {
            //如果删除成功需要调用CoalesceOrRedistribute来进行合并或重分配操作
            leaf_deleted = coalesce_or_redistribute(leaf, transaction, &root_is_latched);
        } else {
            maintain_parent(leaf);
        }
    }

    //根据函数返回结果判断是否有结点需要删除
    if (!leaf_deleted) {
        buffer_pool_manager_->unpin_page(leaf->get_page_id(), true);
        delete leaf;
    }

    if (root_is_latched) {
        root_latch_.unlock();
    }
    return removed;
}

/**
 * @brief 用于处理合并和重分配的逻辑，用于删除键值对后调用
 *
 * @param node 执行完删除操作的结点
 * @param transaction 事务指针
 * @param root_is_latched 传出参数：根节点是否上锁，用于并发操作
 * @return 是否需要删除结点
 * @note User needs to first find the sibling of input page.
 * If sibling's size + input page's size >= 2 * page's minsize, then redistribute.
 * Otherwise, merge(Coalesce).
 */
bool IxIndexHandle::coalesce_or_redistribute(IxNodeHandle *node, Transaction *transaction, bool *root_is_latched) {
    if (node->is_root_page()) {
        //如果是根节点，需要调用AdjustRoot() 函数来进行处理，返回根节点是否需要被删除
        return adjust_root(node);
    }

    //获取node结点的父亲结点
    IxNodeHandle *parent = fetch_node(node->get_parent_page_no());
    int index = parent->find_child(node);
    //寻找node结点的兄弟结点（优先选取前驱结点）
    int neighbor_index = (index == 0) ? 1 : index - 1;
    IxNodeHandle *neighbor = fetch_node(parent->value_at(neighbor_index));
    bool neighbor_is_left = neighbor_index < index;

    bool node_removed = false;
    bool neighbor_removed = false;

    //如果node结点和兄弟结点的键值对数量之和，能够支撑两个B+树结点（即node.size+neighbor.size >=
    //NodeMinSize*2)
    if (neighbor->get_size() + node->get_size() <= neighbor->get_max_size()) {
        //否则需要合并两个结点，将右边的结点合并到左边的结点（调用Coalesce函数）
        if (neighbor_is_left) {
            coalesce(&neighbor, &node, &parent, index, transaction, root_is_latched);
            node_removed = true;
        } else {
            coalesce(&node, &neighbor, &parent, neighbor_index, transaction, root_is_latched);
            neighbor_removed = true;
        }

        if (parent->is_root_page()) {
            adjust_root(parent);
        } else if (parent->get_size() < parent->get_min_size()) {
            coalesce_or_redistribute(parent, transaction, root_is_latched);
        } else {
            maintain_parent(parent);
        }
    } else {
        //则只需要重新分配键值对（调用Redistribute函数）
        redistribute(neighbor, node, parent, index);
        maintain_parent(parent);
    }

    if (!neighbor_removed) {
        buffer_pool_manager_->unpin_page(neighbor->get_page_id(), true);
        delete neighbor;
    }
    buffer_pool_manager_->unpin_page(parent->get_page_id(), true);
    delete parent;

    return node_removed;
}

/**
 * @brief 用于当根结点被删除了一个键值对之后的处理
 * @param old_root_node 原根节点
 * @return bool 根结点是否需要被删除
 * @note size of root page can be less than min size and this method is only called within coalesce_or_redistribute()
 */
bool IxIndexHandle::adjust_root(IxNodeHandle *old_root_node) {
    //如果old_root_node是内部结点，并且大小为1，则直接把它的孩子更新成新的根结点
    if (!old_root_node->is_leaf_page() && old_root_node->get_size() == 1) {
        page_id_t child_page = old_root_node->value_at(0);
        IxNodeHandle *child = fetch_node(child_page);
        child->set_parent_page_no(IX_NO_PAGE);
        update_root_page_no(child_page);
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
        delete child;
        return true;
    }

    //如果old_root_node是叶结点，且大小为0，则直接更新root page
    if (old_root_node->is_leaf_page() && old_root_node->get_size() == 0) {
        update_root_page_no(IX_NO_PAGE);
        file_hdr_->first_leaf_ = IX_NO_PAGE;
        file_hdr_->last_leaf_ = IX_NO_PAGE;
        return true;
    }

    return false;
}

/**
 * @brief 重新分配node和兄弟结点neighbor_node的键值对
 * Redistribute key & value pairs from one page to its sibling page. If index == 0, move sibling page's first key
 * & value pair into end of input "node", otherwise move sibling page's last key & value pair into head of input "node".
 *
 * @param neighbor_node sibling page of input "node"
 * @param node input from method coalesceOrRedistribute()
 * @param parent the parent of "node" and "neighbor_node"
 * @param index node在parent中的rid_idx
 * @note node是之前刚被删除过一个key的结点
 * index=0，则neighbor是node后继结点，表示：node(left)      neighbor(right)
 * index>0，则neighbor是node前驱结点，表示：neighbor(left)  node(right)
 * 注意更新parent结点的相关kv对
 */
void IxIndexHandle::redistribute(IxNodeHandle *neighbor_node, IxNodeHandle *node, IxNodeHandle *parent, int index) {
    //通过index判断neighbor_node是否为node的前驱结点
    if (index > 0) {
        //从neighbor_node中移动一个键值对到node结点中
        int move_idx = neighbor_node->get_size() - 1;
        char *move_key = neighbor_node->get_key(move_idx);
        Rid move_rid = *neighbor_node->get_rid(move_idx);
        node->insert_pairs(0, move_key, &move_rid, 1);
        neighbor_node->erase_pair(move_idx);
        //更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
        parent->set_key(index, node->get_key(0));
        if (!node->is_leaf_page()) {
            maintain_child(node, 0);
        }
    } else {
        //从neighbor_node中移动一个键值对到node结点中
        char *move_key = neighbor_node->get_key(0);
        Rid move_rid = *neighbor_node->get_rid(0);
        node->insert_pairs(node->get_size(), move_key, &move_rid, 1);
        neighbor_node->erase_pair(0);
        //更新父节点中的相关信息，并且修改移动键值对对应孩字结点的父结点信息（maintain_child函数）
        parent->set_key(index + 1, neighbor_node->get_key(0));
        if (!node->is_leaf_page()) {
            maintain_child(node, node->get_size() - 1);
        }
    }
}

/**
 * @brief 合并(Coalesce)函数是将node和其直接前驱进行合并，也就是和它左边的neighbor_node进行合并；
 * 假设node一定在右边。如果上层传入的index=0，说明node在左边，那么交换node和neighbor_node，保证node在右边；合并到左结点，实际上就是删除了右结点；
 * Move all the key & value pairs from one page to its sibling page, and notify buffer pool manager to delete this page.
 * Parent page must be adjusted to take info of deletion into account. Remember to deal with coalesce or redistribute
 * recursively if necessary.
 *
 * @param neighbor_node sibling page of input "node" (neighbor_node是node的前结点)
 * @param node input from method coalesceOrRedistribute() (node结点是需要被删除的)
 * @param parent parent page of input "node"
 * @param index node在parent中的rid_idx
 * @return true means parent node should be deleted, false means no deletion happend
 * @note Assume that *neighbor_node is the left sibling of *node (neighbor -> node)
 */
bool IxIndexHandle::coalesce(IxNodeHandle **neighbor_node, IxNodeHandle **node, IxNodeHandle **parent, int index,
                             Transaction *transaction, bool *root_is_latched) {
    //把node结点的键值对移动到neighbor_node中
    IxNodeHandle *left = *neighbor_node;
    IxNodeHandle *right = *node;

    int left_size = left->get_size();
    int right_size = right->get_size();
    left->insert_pairs(left_size, right->get_key(0), right->get_rid(0), right_size);

    if (left->is_leaf_page()) {
        left->set_next_leaf(right->get_next_leaf());
        if (right->get_next_leaf() != IX_NO_PAGE) {
            IxNodeHandle *next = fetch_node(right->get_next_leaf());
            next->set_prev_leaf(left->get_page_no());
            buffer_pool_manager_->unpin_page(next->get_page_id(), true);
            delete next;
        }
        if (file_hdr_->last_leaf_ == right->get_page_no()) {
            file_hdr_->last_leaf_ = left->get_page_no();
        }
        if (file_hdr_->first_leaf_ == right->get_page_no()) {
            file_hdr_->first_leaf_ = left->get_page_no();
        }
    } else {
        //更新node结点孩子结点的父节点信息（调用maintain_child函数）
        for (int i = left_size; i < left->get_size(); i++) {
            maintain_child(left, i);
        }
    }

    //删除parent中node结点的信息
    int parent_index = (*parent)->find_child(right);
    (*parent)->erase_pair(parent_index);

    maintain_parent(left);

    //释放和删除node结点
    release_node_handle(*right);
    buffer_pool_manager_->unpin_page(right->get_page_id(), true);
    delete right;

    return true;
}

/**
 * @brief 这里把iid转换成了rid，即iid的slot_no作为node的rid_idx(key_idx)
 * node其实就是把slot_no作为键值对数组的下标
 * 换而言之，每个iid对应的索引槽存了一对(key,rid)，指向了(要建立索引的属性首地址,插入/删除记录的位置)
 *
 * @param iid
 * @return Rid
 * @note iid和rid存的不是一个东西，rid是上层传过来的记录位置，iid是索引内部生成的索引槽位置
 */
Rid IxIndexHandle::get_rid(const Iid &iid) const {
    IxNodeHandle *node = fetch_node(iid.page_no);
    if (iid.slot_no >= node->get_size()) {
        buffer_pool_manager_->unpin_page(node->get_page_id(), false);
        throw IndexEntryNotFoundError();
    }
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return *node->get_rid(iid.slot_no);
}

/**
 * @brief FindLeafPage + lower_bound
 *
 * @param key
 * @return Iid
 * @note 上层传入的key本来是int类型，通过(const char *)&key进行了转换
 * 可用*(int *)key转换回去
 */
Iid IxIndexHandle::lower_bound(const char *key) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    if (leaf == nullptr) {
        return Iid{-1, -1};
    }

    int slot = leaf->lower_bound(key);
    Iid iid = {.page_no = leaf->get_page_no(), .slot_no = slot};
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    delete leaf;
    if (root_is_latched) {
        root_latch_.unlock();
    }
    return iid;
}

/**
 * @brief FindLeafPage + upper_bound
 *
 * @param key
 * @return Iid
 */
Iid IxIndexHandle::upper_bound(const char *key) {
    auto [leaf, root_is_latched] = find_leaf_page(key, Operation::FIND, nullptr);
    if (leaf == nullptr) {
        return Iid{-1, -1};
    }

    int slot = leaf->upper_bound(key);
    page_id_t page_no = leaf->get_page_no();
    if (slot == leaf->get_size()) {
        page_id_t next_leaf = leaf->get_next_leaf();
        if (next_leaf != IX_LEAF_HEADER_PAGE && next_leaf != IX_NO_PAGE) {
            page_no = next_leaf;
            slot = 0;
        }
    }
    Iid iid = {.page_no = page_no, .slot_no = slot};
    buffer_pool_manager_->unpin_page(leaf->get_page_id(), false);
    delete leaf;
    if (root_is_latched) {
        root_latch_.unlock();
    }
    return iid;
}

/**
 * @brief 指向最后一个叶子的最后一个结点的后一个
 * 用处在于可以作为IxScan的最后一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_end() const {
    IxNodeHandle *node = fetch_node(file_hdr_->last_leaf_);
    Iid iid = {.page_no = file_hdr_->last_leaf_, .slot_no = node->get_size()};
    buffer_pool_manager_->unpin_page(node->get_page_id(), false);  // unpin it!
    return iid;
}

/**
 * @brief 指向第一个叶子的第一个结点
 * 用处在于可以作为IxScan的第一个
 *
 * @return Iid
 */
Iid IxIndexHandle::leaf_begin() const {
    Iid iid = {.page_no = file_hdr_->first_leaf_, .slot_no = 0};
    return iid;
}

/**
 * @brief 获取一个指定结点
 *
 * @param page_no
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 */
IxNodeHandle *IxIndexHandle::fetch_node(int page_no) const {
    Page *page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    IxNodeHandle *node = new IxNodeHandle(file_hdr_, page);
    
    return node;
}

/**
 * @brief 创建一个新结点
 *
 * @return IxNodeHandle*
 * @note pin the page, remember to unpin it outside!
 * 注意：对于Index的处理是，删除某个页面后，认为该被删除的页面是free_page
 * 而first_free_page实际上就是最新被删除的页面，初始为IX_NO_PAGE
 * 在最开始插入时，一直是create node，那么first_page_no一直没变，一直是IX_NO_PAGE
 * 与Record的处理不同，Record将未插入满的记录页认为是free_page
 */
IxNodeHandle *IxIndexHandle::create_node() {
    IxNodeHandle *node;
    file_hdr_->num_pages_++;

    PageId new_page_id = {.fd = fd_, .page_no = INVALID_PAGE_ID};
    // 从3开始分配page_no，第一次分配之后，new_page_id.page_no=3，file_hdr_.num_pages=4
    Page *page = buffer_pool_manager_->new_page(&new_page_id);
    node = new IxNodeHandle(file_hdr_, page);
    return node;
}

/**
 * @brief 从node开始更新其父节点的第一个key，一直向上更新直到根节点
 *
 * @param node
 */
void IxIndexHandle::maintain_parent(IxNodeHandle *node) {
    IxNodeHandle *curr = node;
    while (curr->get_parent_page_no() != IX_NO_PAGE) {
        // Load its parent
        IxNodeHandle *parent = fetch_node(curr->get_parent_page_no());
        int rank = parent->find_child(curr);
        char *parent_key = parent->get_key(rank);
        char *child_first_key = curr->get_key(0);
        if (memcmp(parent_key, child_first_key, file_hdr_->col_tot_len_) == 0) {
            assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
            break;
        }
        memcpy(parent_key, child_first_key, file_hdr_->col_tot_len_);  // 修改了parent node
        curr = parent;

        assert(buffer_pool_manager_->unpin_page(parent->get_page_id(), true));
    }
}

/**
 * @brief 要删除leaf之前调用此函数，更新leaf前驱结点的next指针和后继结点的prev指针
 *
 * @param leaf 要删除的leaf
 */
void IxIndexHandle::erase_leaf(IxNodeHandle *leaf) {
    assert(leaf->is_leaf_page());

    IxNodeHandle *prev = fetch_node(leaf->get_prev_leaf());
    prev->set_next_leaf(leaf->get_next_leaf());
    buffer_pool_manager_->unpin_page(prev->get_page_id(), true);

    IxNodeHandle *next = fetch_node(leaf->get_next_leaf());
    next->set_prev_leaf(leaf->get_prev_leaf());  // 注意此处是SetPrevLeaf()
    buffer_pool_manager_->unpin_page(next->get_page_id(), true);
}

/**
 * @brief 删除node时，更新file_hdr_.num_pages
 *
 * @param node
 */
void IxIndexHandle::release_node_handle(IxNodeHandle &node) {
    file_hdr_->num_pages_--;
}

/**
 * @brief 将node的第child_idx个孩子结点的父节点置为node
 */
void IxIndexHandle::maintain_child(IxNodeHandle *node, int child_idx) {
    if (!node->is_leaf_page()) {
        //  Current node is inner node, load its child and set its parent to current node
        int child_page_no = node->value_at(child_idx);
        IxNodeHandle *child = fetch_node(child_page_no);
        child->set_parent_page_no(node->get_page_no());
        buffer_pool_manager_->unpin_page(child->get_page_id(), true);
    }
}