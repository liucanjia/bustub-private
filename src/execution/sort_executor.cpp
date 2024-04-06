#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  this->child_executor_->Init();
  this->sort_table_.clear();

  Tuple tuple;
  RID rid;
  while (this->child_executor_->Next(&tuple, &rid)) {
    this->sort_table_.emplace_back(std::make_pair(std::move(tuple), rid));
  }

  auto schema = this->plan_->OutputSchema();
  std::sort(this->sort_table_.begin(), this->sort_table_.end(),
            [&](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) -> bool {
              for (const auto &[orderby_type, expr] : this->plan_->GetOrderBy()) {
                auto a_col_value = expr->Evaluate(&a.first, schema);
                auto b_col_value = expr->Evaluate(&b.first, schema);
                if (orderby_type == OrderByType::ASC || orderby_type == OrderByType::DEFAULT) {
                  if (!a_col_value.CompareExactlyEquals(b_col_value)) {
                    return a_col_value.CompareLessThan(b_col_value) == CmpBool::CmpTrue;
                  }
                } else if (orderby_type == OrderByType::DESC) {
                  if (!a_col_value.CompareExactlyEquals(b_col_value)) {
                    return a_col_value.CompareGreaterThan(b_col_value) == CmpBool::CmpTrue;
                  }
                }
              }
              return false;
            });

  this->iter_ = this->sort_table_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (this->iter_ != this->sort_table_.end()) {
    std::tie(*tuple, *rid) = *this->iter_;
    ++this->iter_;
    return true;
  }

  return false;
}

}  // namespace bustub
