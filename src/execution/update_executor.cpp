//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple update_tuple;
  RID old_rid;
  int updated_count = 0;
  auto target_exprs = this->plan_->target_expressions_;
  auto schema = this->table_info_->schema_;

  while (this->child_executor_->Next(&update_tuple, &old_rid)) {
    // firstly, delete the affected tuple and index
    this->table_info_->table_->UpdateTupleMeta({0, true}, old_rid);
    for (auto index_info : this->table_indexs_) {
      auto key_schema = *index_info->index_->GetKeySchema();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->DeleteEntry(update_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
                                      old_rid, this->exec_ctx_->GetTransaction());
    }

    // update the tuple
    std::vector<Value> tuple_values;
    tuple_values.reserve(target_exprs.size());
    for (const auto &expr : target_exprs) {
      tuple_values.emplace_back(expr->Evaluate(&update_tuple, schema));
    }
    update_tuple = Tuple{tuple_values, &schema};
    // insert a new tuple
    if (auto new_rid = this->table_info_->table_->InsertTuple({0, false}, update_tuple); new_rid != std::nullopt) {
      ++updated_count;
      // update the indexs
      for (auto index_info : this->table_indexs_) {
        auto key_schema = *index_info->index_->GetKeySchema();
        auto key_attrs = index_info->index_->GetKeyAttrs();
        index_info->index_->InsertEntry(update_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
                                        new_rid.value(), this->exec_ctx_->GetTransaction());
      }
    }
  }

  if (!this->update_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(updated_count)}, &this->plan_->OutputSchema()};
    this->update_finished_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
