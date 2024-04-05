//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  this->left_child_->Init();
  this->right_child_->Init();

  this->ht_.clear();
  this->output_tuples_.clear();

  Tuple tuple;
  RID rid;
  while (this->right_child_->Next(&tuple, &rid)) {
    std::vector<Value> keys;
    for (const auto &expr : this->plan_->RightJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&tuple, this->right_child_->GetOutputSchema()));
    }
    this->ht_.emplace(HashJoinKey{keys}, std::move(tuple));
  }
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  auto left_schema = this->left_child_->GetOutputSchema();
  auto right_schema = this->right_child_->GetOutputSchema();
  auto output_schema = this->plan_->OutputSchema();

  while (this->output_tuples_.empty() && this->left_child_->Next(&left_tuple, &left_rid)) {
    std::vector<Value> keys;
    for (const auto &expr : this->plan_->LeftJoinKeyExpressions()) {
      keys.emplace_back(expr->Evaluate(&left_tuple, this->left_child_->GetOutputSchema()));
    }

    auto ht_key = HashJoinKey{keys};
    if (auto iter = this->ht_.find(ht_key); iter != this->ht_.end()) {
      auto [start, end] = this->ht_.equal_range(ht_key);
      for (auto it = start; it != end; it++) {
        this->CreateNewTuple(*tuple, output_schema, &left_tuple, left_schema, &it->second, right_schema);
        this->output_tuples_.emplace_back(std::move(*tuple));
      }
    } else if (this->plan_->GetJoinType() == JoinType::LEFT) {
      this->CreateNewTuple(*tuple, output_schema, &left_tuple, left_schema, nullptr, right_schema);
      return true;
    }
  }

  if (!this->output_tuples_.empty()) {
    *tuple = std::move(this->output_tuples_.back());
    this->output_tuples_.pop_back();
    return true;
  }

  return false;
}

void HashJoinExecutor::CreateNewTuple(Tuple &new_tuple, const Schema &output_schema, const Tuple *left_tuple,
                                      const Schema &left_schema, const Tuple *right_tuple, const Schema &right_schema) {
  std::vector<Value> values;
  values.reserve(left_schema.GetColumnCount() + right_schema.GetColumnCount());
  for (uint32_t i = 0; i < left_schema.GetColumnCount(); i++) {
    values.emplace_back(left_tuple->GetValue(&left_schema, i));
  }

  if (right_tuple == nullptr) {
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
    }
  } else {
    for (uint32_t i = 0; i < right_schema.GetColumnCount(); i++) {
      values.emplace_back(right_tuple->GetValue(&right_schema, i));
    }
  }

  new_tuple = Tuple{values, &output_schema};
}

}  // namespace bustub
