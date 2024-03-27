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

namespace bustub {
static constexpr frame_id_t INVALID_FRAME_ID = -1;  // invalid frame id

void LRUKNode::Insert(size_t new_timestamp) {
  // 页面访问到达k次, 则删除位于队列尾部的最早一次访问的时间戳
  if (this->history_.size() == this->k_) {
    this->history_.pop_back();
  }
  // 将最新一次访问页面的时间戳加入队列头
  this->history_.emplace_front(new_timestamp);
}

void LRUKNode::SetEvictable(bool set_evictable) { this->is_evictable_ = set_evictable; }

auto LRUKNode::GetEvictable() -> bool { return this->is_evictable_; }

auto LRUKNode::GetKDistance(size_t current_timestamp) -> size_t {
  // 页面访问达到k次, 则k-distance为当前时间戳与最早时间戳的差值
  if (this->history_.size() == this->k_) {
    return current_timestamp - this->history_.back();
  }
  // 页面访问不到k次, 则k-distance为+inf
  return std::numeric_limits<size_t>::max();
}

auto LRUKNode::GetEarlyTimeStamp() -> size_t { return this->history_.back(); }

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_{num_frames}, k_{k} {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // 无可驱逐页, 则直接返回false
  if (this->curr_size_ == 0) {
    return false;
  }

  frame_id_t evict_frame_id = INVALID_FRAME_ID;
  size_t evict_distance = 0;
  size_t evict_early_timestamp = std::numeric_limits<size_t>::max();

  std::lock_guard<std::mutex> lk(this->latch_);
  // 遍历寻找最大的k-distance可驱逐页
  for (auto &&[frame_id, node] : this->node_store_) {
    // 不是可驱逐页则跳过
    if (!node.GetEvictable()) {
      continue;
    }

    auto distance = node.GetKDistance(this->current_timestamp_);
    auto early_timestamp = node.GetEarlyTimeStamp();

    if (distance > evict_distance || (distance == evict_distance && early_timestamp < evict_early_timestamp)) {
      evict_frame_id = frame_id;
      evict_distance = distance;
      evict_early_timestamp = early_timestamp;
    }
  }
  // 找到最大k-distance可驱逐页
  if (evict_frame_id != INVALID_FRAME_ID) {
    *frame_id = evict_frame_id;
    this->node_store_.erase(evict_frame_id);
    this->curr_size_--;
    return true;
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  this->CheckFrameId(frame_id, "RecordAccess");

  std::lock_guard<std::mutex> lk(this->latch_);
  this->current_timestamp_++;  // 访问页面时, 时间戳+1

  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    // frmae已存在, 则更新timestamp
    auto &&node = it->second;
    node.Insert(this->current_timestamp_);
  } else {
    // frame不存在, 新建frame entry
    this->node_store_.emplace(frame_id, LRUKNode(this->k_, this->current_timestamp_));
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  this->CheckFrameId(frame_id, "SetEvictable");

  std::lock_guard<std::mutex> lk(this->latch_);
  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    auto &&node = it->second;
    if (node.GetEvictable() != set_evictable) {
      node.SetEvictable(set_evictable);
      if (set_evictable) {
        // 可驱逐页数目增加
        this->curr_size_++;
      } else {
        // 可驱逐页数目减少
        this->curr_size_--;
      }
    }
  } else {
    // frame_id对应的页面不存在, 直接抛出错误
    throw Exception(
        (std::stringstream{} << "At LRUKReplacer::SetEvictable, frame_id: " << frame_id << " entry is not exist.")
            .str());
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  this->CheckFrameId(frame_id, "Remove");

  std::lock_guard<std::mutex> lk(this->latch_);
  if (auto it = this->node_store_.find(frame_id); it != this->node_store_.end()) {
    if (auto &&node = it->second; !node.GetEvictable()) {
      // 驱逐的页面不是可驱逐页, 直接抛出错误
      throw Exception(
          (std::stringstream{} << "At LRUKReplacer::Remove, frame_id: " << frame_id << " is non-evictable.").str());
    }
    this->node_store_.erase(frame_id);
    this->curr_size_--;
  }

  /** If specified frame is not found, directly return from this function. */
}

auto LRUKReplacer::Size() -> size_t { return this->curr_size_; }

}  // namespace bustub
