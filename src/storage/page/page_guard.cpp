#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept { *this = std::move(that); }

void BasicPageGuard::Drop() {
  page_id_t page_id = this->PageId();
  this->bpm_->UnpinPage(page_id, this->is_dirty_);
  this->ClearAll();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;

  that.ClearAll();
  return *this;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (this->page_ != nullptr) {
    this->page_->RLatch();
  }

  auto read_page_guard = ReadPageGuard(this->bpm_, this->page_, this->is_dirty_);
  this->ClearAll();

  return read_page_guard;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (this->page_ != nullptr) {
    this->page_->WLatch();
  }

  auto write_page_guard = WritePageGuard(this->bpm_, this->page_, this->is_dirty_);
  this->ClearAll();

  return write_page_guard;
}

BasicPageGuard::~BasicPageGuard() {
  if (this->page_ != nullptr) {
    this->Drop();
  }
}  // NOLINT

void BasicPageGuard::ClearAll() {
  this->bpm_ = nullptr;
  this->page_ = nullptr;
  this->is_dirty_ = false;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.Drop();
  }
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
    this->guard_.Drop();
  }
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
    this->guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
