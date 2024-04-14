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
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();
  auto txn = this->exec_ctx_->GetTransaction();

  while (!this->table_iter_->IsEnd()) {
    auto [cur_meta, cur_tuple] = this->table_iter_->GetTuple();
    auto cur_rid = this->table_iter_->GetRID();
    ++(*this->table_iter_);

    GetTupleByTimetamp(cur_tuple, cur_meta, cur_rid, schema, txn_mgr, txn);
    if (!cur_meta.is_deleted_ &&
        (filter == nullptr || filter->Evaluate(&cur_tuple, schema).CompareEquals(true_value) == CmpBool::CmpTrue)) {
      *tuple = std::move(cur_tuple);
      *rid = cur_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
