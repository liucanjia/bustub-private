//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  l.unlock();

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  std::unique_lock<std::mutex> commit_lck(this->commit_mutex_);
  txn_ref->read_ts_.store(this->last_commit_ts_);

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  timestamp_t last_commit_ts = this->last_commit_ts_.load() + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  for (auto &&[table_oid, rid_set] : txn->GetWriteSets()) {
    const auto table_info = this->catalog_->GetTable(table_oid);
    for (auto &&rid : rid_set) {
      auto [meta, tuple] = table_info->table_->GetTuple(rid);
      meta.ts_ = last_commit_ts;

      table_info->table_->UpdateTupleInPlace(meta, tuple, rid, nullptr);
    }
  }

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->state_ = TransactionState::COMMITTED;
  txn->commit_ts_.store(last_commit_ts);
  this->last_commit_ts_.store(txn->commit_ts_);
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  auto water_mark = this->GetWatermark();
  //  undo logs are no longer accessible, set the ts_ = INVALID_TS
  for (auto &&table_name : this->catalog_->GetTableNames()) {
    auto table_info = this->catalog_->GetTable(table_name);

    for (auto iter = table_info->table_->MakeIterator(); !iter.IsEnd(); ++iter) {
      bool final_undo_log = false;
      auto [meta, tuple] = iter.GetTuple();
      if (meta.ts_ <= water_mark) {
        final_undo_log = true;
      }

      if (auto opt_undo_link = this->GetUndoLink(tuple.GetRid()); opt_undo_link.has_value()) {
        for (auto undo_link = opt_undo_link.value(); undo_link.IsValid();) {
          if (auto opt_undo_log = this->GetUndoLogOptional(undo_link); opt_undo_log.has_value()) {
            auto undo_log = opt_undo_log.value();
            if (final_undo_log) {
              undo_log.ts_ = INVALID_TS;
              std::unique_lock<std::shared_mutex> lk_txn_map(this->txn_map_mutex_);
              auto txn = this->txn_map_[undo_link.prev_txn_];
              txn->ModifyUndoLog(undo_link.prev_log_idx_, undo_log);
            } else if (undo_log.ts_ <= water_mark) {
              final_undo_log = true;
            }

            undo_link = undo_log.prev_version_;
          } else {
            break;
          }
        }
      }
    }
  }
  // if all undo_logs are unaccessible, remove the transaction
  std::vector<txn_id_t> remove_txn_set;
  std::unique_lock<std::shared_mutex> lk_txn_map(this->txn_map_mutex_);
  for (auto &&[txn_id, txn] : this->txn_map_) {
    if (auto state = txn->GetTransactionState();
        !(state == TransactionState::COMMITTED || state == TransactionState::ABORTED)) {
      continue;
    }
    std::unique_lock<std::mutex> lk_txn(txn->latch_);

    bool remove_flag = true;
    for (const auto &undo_log : txn->undo_logs_) {
      if (undo_log.ts_ != INVALID_TS) {
        remove_flag = false;
      }
    }

    if (remove_flag) {
      remove_txn_set.emplace_back(txn_id);
    }
  }

  for (const auto &txn_id : remove_txn_set) {
    this->txn_map_.erase(txn_id);
  }
}

}  // namespace bustub
