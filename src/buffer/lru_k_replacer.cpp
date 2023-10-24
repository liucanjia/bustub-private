//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <sstream>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {
static constexpr int INVALID_FRAME_ID = -1;  // invalid frame id

void LRUKNode::Insert(size_t new_timestamp) {
  if (this->history_.size() == this->k_) {
    this->history_.pop_back();
  }
  this->history_.push_front(new_timestamp);
}

void LRUKNode::SetEvictable(bool set_evictable) { this->is_evictable_ = set_evictable; }

auto LRUKNode::GetEvictable() -> bool { return this->is_evictable_; }

auto LRUKNode::GetKDistance(size_t current_timestamp) -> size_t {
  if (this->history_.size() == this->k_) {
    return current_timestamp - this->history_.back();
  }
  return SIZE_MAX;
}

auto LRUKNode::GetEarlyTimeStamp() -> size_t { return this->history_.back(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  frame_id_t evict_frame_id = INVALID_FRAME_ID;
  size_t evict_distance = 0;
  size_t evict_early_timestamp = SIZE_MAX;

  std::lock_guard<std::mutex> lk(this->latch_);

  for (auto &&it : this->node_store_) {
    if (!it.second.GetEvictable()) {
      continue;
    }

    auto frame_id = it.first;
    auto distance = it.second.GetKDistance(this->current_timestamp_);
    auto early_timestamp = it.second.GetEarlyTimeStamp();

    if (distance > evict_distance || (distance == evict_distance && early_timestamp < evict_early_timestamp)) {
      evict_frame_id = frame_id;
      evict_distance = distance;
      evict_early_timestamp = early_timestamp;
    }
  }

  if (evict_frame_id != INVALID_FRAME_ID) {
    *frame_id = evict_frame_id;
    this->node_store_.erase(evict_frame_id);
    this->curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  // frame id非法, 直接抛出错误
  if (static_cast<size_t>(frame_id) >= this->replacer_size_) {
    std::stringstream ss;
    ss << "At LRUKReplacer::RecordAccess, frame id:" << frame_id << " is invalid.";
    throw Exception(ss.str());
  }

  std::lock_guard<std::mutex> lk(this->latch_);
  this->current_timestamp_++;

  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    // frmae已存在, 则更新timestamp
    auto &&node = it->second;
    node.Insert(this->current_timestamp_);
  } else {
    // frame不存在, 新建frame entry
    this->node_store_.emplace(std::make_pair(frame_id, LRUKNode(this->k_, this->current_timestamp_)));
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  // frame id非法, 直接抛出错误
  if (static_cast<size_t>(frame_id) >= this->replacer_size_) {
    std::stringstream ss;
    ss << "At LRUKReplacer::SetEvictable, frame id:" << frame_id << " is invalid.";
    throw Exception(ss.str());
  }

  std::lock_guard<std::mutex> lk(this->latch_);

  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    auto &&node = it->second;
    if (node.GetEvictable() != set_evictable) {
      node.SetEvictable(set_evictable);
      if (set_evictable) {
        this->curr_size_++;
      } else {
        this->curr_size_--;
      }
    }
  } else {
    std::stringstream ss;
    ss << "At LRUKReplacer::SetEvictable, frame id:" << frame_id << " entry is not exist.";
    throw Exception(ss.str());
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lk(this->latch_);

  std::stringstream ss;
  if (static_cast<size_t>(frame_id) >= this->replacer_size_) {
    // frame id非法, 直接抛出错误
    ss << "At LRUKReplacer::Remove, frame id:" << frame_id << " is invalid.";
    throw Exception(ss.str());
  }

  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    this->node_store_.erase(frame_id);
    this->curr_size_--;
  } else {
    /** If specified frame is not found, directly return from this function. */
    return;
  }
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
