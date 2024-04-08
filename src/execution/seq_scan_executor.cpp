//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_iter_ = std::make_unique<TableIterator>(this->table_info_->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto true_value = ValueFactory::GetBooleanValue(true);
  auto filter = this->plan_->filter_predicate_;
  auto schema = this->table_info_->schema_;

  while (!this->table_iter_->IsEnd()) {
    auto [cur_meta, cur_tuple] = this->table_iter_->GetTuple();
    auto cur_rid = this->table_iter_->GetRID();
    ++(*this->table_iter_);

    this->UpdateTupleByTimetamp(cur_tuple, cur_meta, cur_rid);
    if (!cur_meta.is_deleted_ &&
        (filter == nullptr || filter->Evaluate(&cur_tuple, schema).CompareEquals(true_value) == CmpBool::CmpTrue)) {
      *tuple = std::move(cur_tuple);
      *rid = cur_rid;
      return true;
    }
  }
  return false;
}

void SeqScanExecutor::UpdateTupleByTimetamp(Tuple &tuple, TupleMeta &tuple_meta, RID &rid) {
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();
  auto txn = this->exec_ctx_->GetTransaction();

  // The tuple modified by another uncommitted transaction or The tuple newer than the transaction read timestamp
  if ((tuple_meta.ts_ >= TXN_START_ID && tuple_meta.ts_ != txn->GetTransactionId()) ||
      (tuple_meta.ts_ < TXN_START_ID && tuple_meta.ts_ > txn->GetReadTs())) {
    std::vector<UndoLog> undo_logs;

    bool final_log = false;
    if (auto opt_undo_link = txn_mgr->GetUndoLink(rid); opt_undo_link.has_value()) {
      for (auto undo_link = opt_undo_link.value(); undo_link.IsValid() && !final_log;) {
        if (auto opt_undo_log = txn_mgr->GetUndoLogOptional(undo_link); opt_undo_log.has_value()) {
          auto undo_log = opt_undo_log.value();
          undo_logs.emplace_back(undo_log);
          undo_link = undo_log.prev_version_;
          if (undo_log.ts_ <= txn->GetReadTs()) {
            final_log = true;
          }
        }
      }
    }

    if (final_log) {
      auto opt_tuple = ReconstructTuple(&this->plan_->OutputSchema(), tuple, tuple_meta, undo_logs);
      if (opt_tuple.has_value()) {
        tuple = opt_tuple.value();
        tuple_meta.is_deleted_ = undo_logs.back().is_deleted_;
        tuple_meta.ts_ = undo_logs.back().ts_;
        return;
      }
    }
    // The tuple was deleted or the Transaction read timestamp older than all tuple
    tuple_meta.is_deleted_ = true;
    tuple_meta.ts_ = 0;
  }

  // The tuple in the table heap is the most recent data or The tuple in the table heap contains modification by the
  // current transaction
}

}  // namespace bustub
