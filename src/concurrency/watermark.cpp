#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!
  if (auto iter = this->current_reads_.find(read_ts); iter != this->current_reads_.end()) {
    ++iter->second;
  } else {
    this->current_reads_[read_ts] = 1;
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (auto cnt = --this->current_reads_[read_ts]; cnt == 0) {
    this->current_reads_.erase(read_ts);

    if (read_ts == this->watermark_) {
      if (auto iter = this->current_reads_.begin(); iter != this->current_reads_.end()) {
        this->watermark_ = iter->first;
      } else {
        this->watermark_ = this->commit_ts_;
      }
    }
  }
}

}  // namespace bustub
