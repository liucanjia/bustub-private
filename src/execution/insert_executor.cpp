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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
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
  auto schema = this->table_info_->schema_;
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();

  while (this->child_executor_->Next(&insert_tuple, &old_rid)) {
    ++insertd_count;
    InsertTuple(insert_tuple, schema, this->table_info_, this->table_indexs_, txn, txn_mgr);
  }

  if (!this->insert_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(insertd_count)}, &this->GetOutputSchema()};
    this->insert_finished_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
