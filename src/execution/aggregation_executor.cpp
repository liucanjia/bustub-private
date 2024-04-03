//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  this->child_executor_->Init();

  this->aht_.Clear();
  Tuple cur_tuple;
  RID cur_rid;
  while (this->child_executor_->Next(&cur_tuple, &cur_rid)) {
    auto aggregate_key = this->MakeAggregateKey(&cur_tuple);
    auto aggregate_value = this->MakeAggregateValue(&cur_tuple);
    this->aht_.InsertCombine(aggregate_key, aggregate_value);
  }

  this->aht_iterator_ = this->aht_.Begin();
  if (this->aht_iterator_ == this->aht_.End() && this->plan_->GetGroupBys().empty()) {
    this->aht_.InitEmpty();
    this->aht_iterator_ = this->aht_.Begin();
  }
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->aht_iterator_ != this->aht_.End()) {
    auto kv_pair = std::vector<Value>(this->aht_iterator_.Key().group_bys_);
    auto val = this->aht_iterator_.Val().aggregates_;
    kv_pair.insert(kv_pair.end(), val.begin(), val.end());
    *tuple = Tuple{std::vector<Value>{kv_pair}, &this->plan_->OutputSchema()};
    ++this->aht_iterator_;
    return true;
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
