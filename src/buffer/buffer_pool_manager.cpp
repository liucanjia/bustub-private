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
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
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
  std::unique_lock<std::mutex> lk(this->latch_);
  frame_id_t frame_id = INVALID_PAGE_ID;

  if (!this->free_list_.empty()) {
    // buffer pool isn't full, get a free frame
    frame_id = this->free_list_.back();
    this->free_list_.pop_back();
  } else {
    // buffer poll is full, try to evict a old page
    if (auto evict_able = this->replacer_->Evict(&frame_id); evict_able) {
      // find one page can be evict
      auto page = &this->pages_[frame_id];
      this->page_table_.erase(page->GetPageId());
      // if page is dirty, write back to disk and reset the page object
      if (page->IsDirty()) {
        auto promise = this->disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        this->disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
        future.get();
        page->ResetPage();
      }
    } else {
      // all page are not evictable, return nullptr
      return nullptr;
    }
  }
  // get a new page id and pin the page
  *page_id = this->AllocatePage();
  this->page_table_[*page_id] = frame_id;
  if (!this->PinPage(*page_id)) {
    std::stringstream ss;
    ss << "Try to pin page " << *page_id << " fail.";
    throw ExecutionException(ss.str());
  }

  return &this->pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::unique_lock<std::mutex> lk(this->latch_);
  frame_id_t frame_id = INVALID_FRAME_ID;
  // if page in the buffer pool, pin the page and return the page
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    frame_id = it->second;
    this->PinPage(page_id);
    return &this->pages_[frame_id];
  }

  // if page in the disk, try to read it from disk
  if (!this->free_list_.empty()) {
    // buffer pool isn't full, get a free frame
    frame_id = this->free_list_.back();
    this->free_list_.pop_back();
  } else {
    // buffer poll is full, try to evict a old page
    if (auto evict_able = this->replacer_->Evict(&frame_id); evict_able) {
      // find one page can be evict
      auto page = &this->pages_[frame_id];
      this->page_table_.erase(page->GetPageId());
      // if page is dirty, write back to disk and reset the page object
      if (page->IsDirty()) {
        auto promise = this->disk_scheduler_->CreatePromise();
        auto future = promise.get_future();
        this->disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
        future.get();
        page->ResetPage();
      }
    } else {
      // all page are not evictable, return nullptr
      return nullptr;
    }
  }

  this->page_table_[page_id] = frame_id;
  auto page = &this->pages_[frame_id];
  auto promise = this->disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  this->disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(promise)});
  future.get();
  if (!this->PinPage(page_id)) {
    std::stringstream ss;
    ss << "Try to pin page " << page_id << " fail.";
    throw ExecutionException(ss.str());
  }

  return &this->pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  // if find the page in buffer pool, try to decrease the pin count and set dirty flag
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto page = &this->pages_[frame_id];
    if (page->GetPinCount() > 0) {
      page->DecPinCount();
      page->SetDirty(is_dirty);
      if (page->GetPinCount() == 0) {
        this->replacer_->SetEvictable(frame_id, true);
      }
    }
    return true;
  }
  // If page_id is not in the buffer pool or its pin count is already return false.
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto page = &this->pages_[frame_id];
    auto promise = this->disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    this->disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    future.get();

    page->SetDirty(false);
    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lk(this->latch_);
  for (auto &it : this->page_table_) {
    auto page_id = it.first;
    auto frame_id = it.second;
    auto page = &this->pages_[frame_id];
    auto promise = this->disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    this->disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    future.get();

    page->SetDirty(false);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto page = &this->pages_[frame_id];
    if (page->GetPinCount() != 0) {
      return false;
    }

    this->page_table_.erase(page_id);
    this->replacer_->Remove(frame_id);
    this->free_list_.push_front(frame_id);
    page->ResetPage();
    this->DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::PinPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> bool {
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    // page is exist, increase the pin count of the page
    auto frame_id = it->second;
    auto page = &this->pages_[frame_id];
    if (page->GetPinCount() == 0) {
      // if it's first time to pin the page, disable the eviction of the page
      page->SetPageId(page_id);
      this->replacer_->RecordAccess(frame_id);
      this->replacer_->SetEvictable(frame_id, false);
    } else {
      this->replacer_->RecordAccess(frame_id);
    }
    page->IncPinCount();

    return true;
  }
  // page is not in the buffer pool, return false;
  return false;
}
}  // namespace bustub
