//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  this->max_depth_ = max_depth;
  this->global_depth_ = 0;
  std::fill(std::begin(this->local_depths_), std::end(this->local_depths_), 0);
  std::fill(std::begin(this->bucket_page_ids_), std::end(this->bucket_page_ids_), INVALID_PAGE_ID);
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & ((1 << this->global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx, uint32_t bound_depth) const -> uint32_t {
  uint32_t mid = 1 << (bound_depth - 1);
  if (bucket_idx < mid) {
    return bucket_idx + mid;
  }

  return bucket_idx - mid;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t { return (1 << this->global_depth_) - 1; }

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (1 << this->local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return this->global_depth_; }

auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t { return this->max_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (this->global_depth_ >= this->max_depth_) {
    return;
  }

  size_t count = 1 << this->global_depth_;
  for (size_t i = 0; i < count; i++) {
    auto split_image_index = this->GetSplitImageIndex(i, this->global_depth_ + 1);
    this->bucket_page_ids_[split_image_index] = this->bucket_page_ids_[i];
    this->local_depths_[split_image_index] = this->local_depths_[i];
  }
  this->global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  if (this->global_depth_ > 0) {
    this->global_depth_--;
  }
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  auto first = std::begin(this->local_depths_);
  auto last = first + this->Size();
  return std::all_of(first, last, [=](auto depth) { return depth < this->global_depth_; });
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return 1 << this->global_depth_; }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return 1 << this->max_depth_; }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return this->local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  if (local_depth > this->max_depth_) {
    return;
  }
  this->local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (this->local_depths_[bucket_idx] >= this->max_depth_) {
    return;
  }
  this->local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (this->local_depths_[bucket_idx] > 0) {
    this->local_depths_[bucket_idx]--;
  }
}
}  // namespace bustub
