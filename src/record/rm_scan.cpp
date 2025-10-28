/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化file_handle，rid设置为无效值
    rid_.page_no = RM_NO_PAGE;
    rid_.slot_no = -1;
    
    // 定位到第一个有效记录
    next();
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next() {
    // 如果已经到达文件末尾，直接返回
    if (is_end()) {
        return;
    }
    
    // 获取文件中的页数
    int num_pages = file_handle_->file_hdr_.num_pages;
    
    // 确定起始搜索位置
    int start_page, start_slot;
    if (rid_.page_no == RM_NO_PAGE || rid_.slot_no == -1) {
        // 第一次调用，从第一页第一个slot开始
        start_page = RM_FIRST_RECORD_PAGE;
        start_slot = 0;
    } else {
        // 从当前rid的下一个位置开始
        start_page = rid_.page_no;
        start_slot = rid_.slot_no + 1;
    }
    
    // 遍历所有页面寻找下一个有效记录
    for (int page_no = start_page; page_no < num_pages; ++page_no) {
        // 获取当前页的句柄
        RmPageHandle page_handle = file_handle_->fetch_page_handle(page_no);
        if (page_handle.page == nullptr) {
            continue; // 页面获取失败，跳过
        }
        
        // 获取当前页的slot数量
        int num_slots = file_handle_->file_hdr_.num_records_per_page;
        
        // 确定在当前页中的起始slot
        int slot_start = (page_no == start_page) ? start_slot : 0;
        
        // 使用bitmap高效查找下一个有效记录
        int next_slot = Bitmap::next_bit(true, page_handle.bitmap, num_slots, slot_start - 1);
        
        if (next_slot < num_slots) {
            // 找到有效记录
            rid_.page_no = page_no;
            rid_.slot_no = next_slot;
            file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
            return;
        }
        
        // 当前页没有更多记录，继续查找下一页
        file_handle_->buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    }
    
    // 没有找到更多记录，设置rid为文件末尾
    rid_.page_no = num_pages;
    rid_.slot_no = 0;
}

/**
 * @brief 判断是否到达文件末尾
 */
bool RmScan::is_end() const {
    // 如果rid指向的页号大于等于文件总页数，说明到达文件末尾
    return rid_.page_no >= file_handle_->file_hdr_.num_pages;
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const {
    return rid_;
}