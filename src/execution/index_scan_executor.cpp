//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto catalog = this->exec_ctx_->GetCatalog();
  auto index_oid = this->plan_->GetIndexOid();
  auto index_info = catalog->GetIndex(index_oid);
  auto schema = catalog->GetTable(this->plan_->table_oid_)->schema_;
  auto key_schema = index_info->key_schema_;
  if (this->plan_->pred_key_ != nullptr) {
    auto key_tuple = Tuple{{this->plan_->pred_key_->Evaluate(nullptr, schema)}, &key_schema};
    index_info->index_->ScanKey(key_tuple, &this->rids_, this->exec_ctx_->GetTransaction());
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!this->rids_.empty()) {
    auto table_oid = this->plan_->table_oid_;
    auto table_info = this->exec_ctx_->GetCatalog()->GetTable(table_oid);
    auto schema = this->plan_->OutputSchema();
    auto txn = this->exec_ctx_->GetTransaction();
    auto txn_mgr = this->exec_ctx_->GetTransactionManager();

    *rid = this->rids_.back();
    this->rids_.pop_back();

    TupleMeta meta;
    std::tie(meta, *tuple) = table_info->table_->GetTuple(*rid);
    GetTupleByTimetamp(*tuple, meta, *rid, schema, txn_mgr, txn);
    return !meta.is_deleted_;
  }

  return false;
}

}  // namespace bustub
