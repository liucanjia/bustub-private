//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }

  this->plan_ = plan;
  this->left_executer_ = std::move(left_executor);
  this->right_executer_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  this->left_executer_->Init();
  this->right_executer_->Init();
  this->left_tuples_.clear();

  Tuple tuple;
  RID rid;
  while (this->left_executer_->Next(&tuple, &rid)) {
    this->left_tuples_.emplace_back(tuple);
  }

  this->left_iter_ = this->left_tuples_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto left_schema = this->left_executer_->GetOutputSchema();
  auto right_schema = this->right_executer_->GetOutputSchema();
  auto output_schema = this->plan_->OutputSchema();
  auto filter_expr = this->plan_->predicate_;

  Tuple right_tuple;
  RID right_rid;
  while (this->left_iter_ != this->left_tuples_.end()) {
    auto left_tuple = *this->left_iter_;
    if (this->right_executer_->Next(&right_tuple, &right_rid)) {
      auto value = filter_expr->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (!value.IsNull() && value.GetAs<bool>()) {
        this->left_join_flag_ = true;
        this->CreateNewTuple(*tuple, output_schema, &left_tuple, left_schema, &right_tuple, right_schema);
        return true;
      }
    } else {
      this->right_executer_->Init();
      ++this->left_iter_;
      if (!this->left_join_flag_ && this->plan_->GetJoinType() == JoinType::LEFT) {
        this->CreateNewTuple(*tuple, output_schema, &left_tuple, left_schema, nullptr, right_schema);
        return true;
      }
      this->left_join_flag_ = false;
    }
  }

  return false;
}

void NestedLoopJoinExecutor::CreateNewTuple(Tuple &new_tuple, const Schema &output_schema, const Tuple *left_tuple,
                                            const Schema &left_schema, const Tuple *right_tuple,
                                            const Schema &right_schema) {
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
