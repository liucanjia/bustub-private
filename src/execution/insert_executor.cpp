//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // init the child_executor first
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple insert_tuple;
  RID old_rid;
  int insertd_count = 0;
  auto timestamp = this->exec_ctx_->GetTransaction()->GetTransactionId();
  auto txn = this->exec_ctx_->GetTransaction();

  while (this->child_executor_->Next(&insert_tuple, &old_rid)) {
    if (auto new_rid = this->table_info_->table_->InsertTuple({timestamp, false}, insert_tuple);
        new_rid != std::nullopt) {
      ++insertd_count;
      // update the transaction write set
      txn->AppendWriteSet(this->table_info_->oid_, new_rid.value());
      // update the indexs
      for (auto index_info : this->table_indexs_) {
        auto key_schema = *index_info->index_->GetKeySchema();
        auto key_attrs = index_info->index_->GetKeyAttrs();
        index_info->index_->InsertEntry(insert_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
                                        new_rid.value(), this->exec_ctx_->GetTransaction());
      }
    }
  }

  if (!this->insert_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(insertd_count)}, &this->GetOutputSchema()};
    this->insert_finished_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
