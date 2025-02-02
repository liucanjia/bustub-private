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
static constexpr int INVALID_FRAME_ID = -1;  // invalid frame id

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
  frame_id_t frame_id = INVALID_FRAME_ID;

  if (!this->TryEvict(&frame_id)) {
    // all page are not evictable, return nullptr
    *page_id = INVALID_PAGE_ID;
    return nullptr;
  }
  // create a new page on the disk and pin the page(because new page is clean, don't need to read page from disk to
  // buffer_pool)
  *page_id = this->AllocatePage();
  this->page_table_[*page_id] = frame_id;
  if (!this->PinPage(*page_id)) {
    throw ExecutionException((std::stringstream{} << "Try to pin page " << *page_id << " fail.").str());
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
  if (!this->TryEvict(&frame_id)) {
    // all page are not evictable, return nullptr
    return nullptr;
  }
  // pin the page
  this->page_table_[page_id] = frame_id;
  auto &page = this->pages_[frame_id];
  if (!this->PinPage(page_id)) {
    throw ExecutionException((std::stringstream{} << "Try to pin page " << page_id << " fail.").str());
  }
  // read the page data from disk to buffer_pool
  this->RWDisk(page, false);
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  // if find the page in buffer pool, try to decrease the pin_count and set dirty flag
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto &page = this->pages_[frame_id];
    if (page.GetPinCount() > 0) {
      page.pin_count_--;
      /** only allow set dirty flag: false->true.
       *   For example:
       *     A thread write the page, set dirty flag to true,
       *     and then B thread read the page, set dirty flag to false,
       *     now the page has been modified, but dirty flag is false.
       */
      if (!page.is_dirty_ && is_dirty) {
        page.is_dirty_ = is_dirty;
      }
      if (page.GetPinCount() == 0) {
        this->replacer_->SetEvictable(frame_id, true);
      }
      return true;
    }
  }
  // If page_id is not in the buffer pool or its pin count is already 0, return false.
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto &page = this->pages_[frame_id];
    this->RWDisk(page, true);

    page.is_dirty_ = false;
    return true;
  }

  return false;
}

void BufferPoolManager::FlushAllPages() {
  std::unique_lock<std::mutex> lk(this->latch_);
  for (auto &[page_id, frame_id] : this->page_table_) {
    auto &page = this->pages_[frame_id];
    this->RWDisk(page, true);

    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lk(this->latch_);
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    auto frame_id = it->second;
    auto &page = this->pages_[frame_id];
    if (page.GetPinCount() != 0) {
      return false;
    }

    this->page_table_.erase(page_id);
    this->replacer_->Remove(frame_id);
    this->free_list_.push_front(frame_id);
    this->ResetPage(page);
    this->DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, this->FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto page = this->FetchPage(page_id);
  page->RLatch();
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto page = this->FetchPage(page_id);
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, this->NewPage(page_id)}; }

auto BufferPoolManager::PinPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> bool {
  if (auto it = this->page_table_.find(page_id); it != this->page_table_.end()) {
    // page is exist, increase the pin count of the page
    auto frame_id = it->second;
    auto &page = this->pages_[frame_id];
    // update the frame timestamp
    this->replacer_->RecordAccess(frame_id);
    if (page.GetPinCount() == 0) {
      // if it's first time to pin the page, disable the eviction of the page
      page.page_id_ = page_id;
      this->replacer_->SetEvictable(frame_id, false);
    }
    // update the pin_count
    page.pin_count_++;

    return true;
  }
  // page is not in the buffer pool, return false;
  return false;
}

auto BufferPoolManager::TryEvict(frame_id_t *frame_id) -> bool {
  if (!this->free_list_.empty()) {
    // buffer pool isn't full, get a free frame
    *frame_id = this->free_list_.back();
    this->free_list_.pop_back();
  } else if (auto evict_able = this->replacer_->Evict(frame_id); evict_able) {
    // buffer pool is full, try to evict a old page
    auto &page = this->pages_[*frame_id];
    this->page_table_.erase(page.GetPageId());
    // if page is dirty, write back to disk and reset the page object
    if (page.IsDirty()) {
      this->RWDisk(page, true);
      this->ResetPage(page);
    }
  } else {
    // all page are not evictable, return false
    return false;
  }

  return true;
}

void BufferPoolManager::ResetPage(Page &page) {
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page.ResetMemory();
}

void BufferPoolManager::RWDisk(Page &page, bool is_write_) {
  auto promise = this->disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  this->disk_scheduler_->Schedule({is_write_, page.GetData(), page.GetPageId(), std::move(promise)});
  future.get();
}
}  // namespace bustub
