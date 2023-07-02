//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t fid = -1;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
  } else {
    if (!replacer_->Evict(&fid)) {
      // free list is empty and no evictable
      return nullptr;
    }
  }
  Page *ptr = pages_ + fid;
  // flush dirty page
  if (ptr->is_dirty_) {
    disk_manager_->WritePage(ptr->page_id_, ptr->data_);
  }
  // reset memory and metadata
  if (ptr->page_id_ != INVALID_PAGE_ID) {  // remove relation between old page id and fid
    page_table_.erase(ptr->page_id_);
  }
  ptr->ResetMemory();
  ptr->page_id_ = AllocatePage();
  ptr->pin_count_ = 1;
  ptr->is_dirty_ = false;
  page_table_[ptr->page_id_] = fid;
  // lru policy helper
  replacer_->SetEvictable(fid, false);  // Pin page
  replacer_->RecordAccess(fid);

  *page_id = ptr->page_id_;
  return ptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t fid = -1;
  Page *ptr = nullptr;
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    fid = iter->second;
    ptr = pages_ + fid;
    ++ptr->pin_count_;  // already pinned, increase pin count
  } else {
    // alloc new page, same as NewPage()
    if (!free_list_.empty()) {
      fid = free_list_.back();
      free_list_.pop_back();
    } else {
      if (!replacer_->Evict(&fid)) {
        // free list is empty and no evictable
        return nullptr;
      }
    }
    ptr = pages_ + fid;
    // flush dirty page
    if (ptr->is_dirty_) {
      disk_manager_->WritePage(ptr->page_id_, ptr->data_);
    }
    // reset memory and metadata
    if (ptr->page_id_ != INVALID_PAGE_ID) {  // remove relation between old page id and fid
      page_table_.erase(ptr->page_id_);
    }
    ptr->ResetMemory();
    ptr->page_id_ = page_id;
    ptr->pin_count_ = 1;
    ptr->is_dirty_ = false;
    page_table_[ptr->page_id_] = fid;
    // load page from dict
    disk_manager_->ReadPage(ptr->page_id_, ptr->data_);
  }

  // lru policy helper
  replacer_->SetEvictable(fid, false);  // Pin page
  replacer_->RecordAccess(fid);
  return ptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto ptr = pages_ + iter->second;
  if (ptr->pin_count_ == 0) {
    return false;
  }
  --ptr->pin_count_;
  ptr->is_dirty_ = (ptr->is_dirty_) || is_dirty;
  if (ptr->pin_count_ == 0) {
    replacer_->SetEvictable(iter->second, true);  // unpin page
  }
  // here, we donot remove pid -> fid relation from page_table
  // because unpinned page could be fetched again if reused quickly!
  // so we has to mannually remove the relation when the page_id is changged
  // and old relation no longer exist ie: NewPage, FetchPage
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  auto ptr = pages_ + iter->second;
  disk_manager_->WritePage(ptr->page_id_, ptr->data_);
  ptr->is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto ptr = pages_; ptr != pages_ + pool_size_; ++ptr) {
    if (ptr->page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(ptr->page_id_, ptr->data_);
      ptr->is_dirty_ = false;
    }
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  auto fid = iter->second;
  auto ptr = pages_ + fid;
  if (ptr->pin_count_ > 0) {
    return false;
  }
  // page is unpined, release it
  // reset data and meta, to be a fresh clean page
  if (ptr->is_dirty_) {
    disk_manager_->WritePage(ptr->page_id_, ptr->data_);
  }
  ptr->ResetMemory();
  ptr->page_id_ = INVALID_PAGE_ID;
  ptr->pin_count_ = 0;
  ptr->is_dirty_ = false;
  // remove relatioin, link to freelist, stop lru watch
  page_table_.erase(page_id);
  free_list_.emplace_back(static_cast<int>(fid));
  replacer_->Remove(fid);
  // free page?
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
