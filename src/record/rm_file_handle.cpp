/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    // 检查RID有效性
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr_.num_pages) {
        throw std::runtime_error("Invalid page number");
    }
    
    // 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查slot_no有效性
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Invalid slot number");
    }
    
    // 检查该slot是否有记录
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record not exists");
    }
    
    // 初始化一个指向RmRecord的指针（赋值其内部的data和size）
    char* slot_data = page_handle.get_slot(rid.slot_no);
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size, slot_data);
    
    // 释放页面
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    if (buf == nullptr) {
        throw std::runtime_error("Buffer is null");
    }
    
    // 获取当前未满的page handle
    RmPageHandle page_handle = create_page_handle();
    int page_no = page_handle.page->get_page_id().page_no;
    
    // 在page handle中找到空闲slot位置
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);
    if (slot_no == file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("No free slot found in page");
    }
    
    // 将buf复制到空闲slot位置
    char* slot = page_handle.get_slot(slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 更新page_handle.page_hdr中的数据结构
    Bitmap::set(page_handle.bitmap, slot_no);
    page_handle.page_hdr->num_records++;
    
    bool page_was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
    
    // 注意考虑插入一条记录后页面已满的情况，需要更新file_hdr_.first_free_page_no
    if (page_was_full) {
        // 页面已满，从空闲链表中移除
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
        // 将文件头写回磁盘
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    }
    
    // 标记页面为dirty
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
    
    return Rid{page_no, slot_no};
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    if (buf == nullptr) {
        throw std::runtime_error("Buffer is null");
    }
    
    // 检查RID有效性
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr_.num_pages) {
        throw std::runtime_error("Invalid page number");
    }
    
    // 获取指定页面
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查slot_no有效性
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Invalid slot number");
    }
    
    // 检查该slot是否已被占用
    if (Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Slot already occupied");
    }
    
    // 将数据复制到指定slot
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 更新bitmap和记录数
    Bitmap::set(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records++;
    
    bool page_was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
    
    // 如果页面因此变为满页，更新文件头
    if (page_was_full) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;
        page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
        disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    }
    
    // 标记页面为dirty
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    // 检查RID有效性
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr_.num_pages) {
        throw std::runtime_error("Invalid page number");
    }
    
    // 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查slot_no有效性
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Invalid slot number");
    }
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record not exists");
    }
    
    bool was_full = (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page);
    
    // 更新page_handle.page_hdr中的数据结构
    Bitmap::reset(page_handle.bitmap, rid.slot_no);
    page_handle.page_hdr->num_records--;
    
    // 注意考虑删除一条记录后页面从未满变成未满的情况，需要调用release_page_handle()
    if (was_full) {
        release_page_handle(page_handle);
    }
    
    // 标记页面为dirty
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    if (buf == nullptr) {
        throw std::runtime_error("Buffer is null");
    }
    
    // 检查RID有效性
    if (rid.page_no < RM_FIRST_RECORD_PAGE || rid.page_no >= file_hdr_.num_pages) {
        throw std::runtime_error("Invalid page number");
    }
    
    // 获取指定记录所在的page handle
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    
    // 检查slot_no有效性
    if (rid.slot_no < 0 || rid.slot_no >= file_hdr_.num_records_per_page) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Invalid slot number");
    }
    
    // 检查记录是否存在
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
        throw std::runtime_error("Record not exists");
    }
    
    // 更新记录
    char* slot = page_handle.get_slot(rid.slot_no);
    memcpy(slot, buf, file_hdr_.record_size);
    
    // 标记页面为dirty
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);
}

/**
 * 以下函数为辅助函数，仅提供参考，可以选择完成如下函数，也可以删除如下函数，在单元测试中不涉及如下函数接口的直接调用
*/

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    // if page_no is invalid, throw PageNotExistError exception
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw std::runtime_error("Page not exists");
    }
    
    // 使用缓冲池获取指定页面，并生成page_handle返回给上层
    Page* page = buffer_pool_manager_->fetch_page(PageId{fd_, page_no});
    if (page == nullptr) {
        throw std::runtime_error("Failed to fetch page");
    }
    
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    // 使用缓冲池来创建一个新page
    PageId page_id{fd_, 0};  
    Page* page = buffer_pool_manager_->new_page(&page_id);
    if (page == nullptr) {
        throw std::runtime_error("Failed to create new page");
    }
    
    int page_no = page_id.page_no;
    
    // 更新page handle中的相关信息
    RmPageHandle page_handle(&file_hdr_, page);
    
    // 初始化页面头
    page_handle.page_hdr->num_records = 0;
    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    
    // 初始化bitmap为全0
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);
    
    // 更新file_hdr_
    file_hdr_.num_pages++;
    
    // 将新页面加入到空闲链表头部
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_no;
    
    // 将文件头写回磁盘
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
    
    return page_handle;
}

/**
 * @brief 创建或获取一个空闲的page handle
 *
 * @return RmPageHandle 返回生成的空闲page handle
 * @note pin the page, remember to unpin it outside!
 */
RmPageHandle RmFileHandle::create_page_handle() {
    // 判断file_hdr_中是否还有空闲页
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        // 没有空闲页：使用缓冲池来创建一个新page；可直接调用create_new_page_handle()
        return create_new_page_handle();
    } else {
        // 有空闲页：直接获取第一个空闲页
        // 检查空闲页号是否有效
        if (file_hdr_.first_free_page_no < 0 || file_hdr_.first_free_page_no >= file_hdr_.num_pages) {
            throw std::runtime_error("Invalid free page number");
        }
        return fetch_page_handle(file_hdr_.first_free_page_no);
    }
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    // 当page从已满变成未满，考虑如何更新：
    // 1. page_handle.page_hdr->next_free_page_no
    // 2. file_hdr_.first_free_page_no
    
    // 将当前页面加入到空闲链表头部
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
    
    // 将文件头写回磁盘
    disk_manager_->write_page(fd_, RM_FILE_HDR_PAGE, (char*)&file_hdr_, sizeof(file_hdr_));
}