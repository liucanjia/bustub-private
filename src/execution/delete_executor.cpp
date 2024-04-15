//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = this->table_info_->schema_;
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();

  std::vector<std::pair<Tuple, RID>> delete_tuples;
  while (this->child_executor_->Next(tuple, rid)) {
    delete_tuples.emplace_back(std::make_pair(std::move(*tuple), *rid));
  }

  for (auto &&[delete_tuple, delete_rid] : delete_tuples) {
    DeleteTuple(delete_tuple, delete_rid, schema, this->table_info_, txn, txn_mgr);
  }

  if (!this->delete_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(delete_tuples.size())}, &this->GetOutputSchema()};
    this->delete_finished_ = true;
    return true;
  }

  return false;
}

// auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   Tuple delete_tuple;
//   RID delete_rid;
//   int deleted_count = 0;

//   while (this->child_executor_->Next(&delete_tuple, &delete_rid)) {
//     // delete tuple
//     this->table_info_->table_->UpdateTupleMeta({0, true}, delete_rid);
//     ++deleted_count;

//     // update the indexs
//     for (auto index_info : this->table_indexs_) {
//       auto key_schema = *index_info->index_->GetKeySchema();
//       auto key_attrs = index_info->index_->GetKeyAttrs();
//       index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
//                                       delete_rid, this->exec_ctx_->GetTransaction());
//     }
//   }

//   if (!this->delete_finished_) {
//     *tuple = Tuple{{ValueFactory::GetIntegerValue(deleted_count)}, &this->GetOutputSchema()};
//     this->delete_finished_ = true;
//     return true;
//   }

//   return false;
// }

}  // namespace bustub
