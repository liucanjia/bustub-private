#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  this->child_executor_->Init();
  this->top_n_result_.clear();

  auto schema = this->plan_->OutputSchema();
  auto top_n_cmp = [&](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) -> bool {
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
  };

  std::priority_queue<std::pair<Tuple, RID>, std::vector<std::pair<Tuple, RID>>, decltype(top_n_cmp)> top_n_pq(
      top_n_cmp);

  Tuple tuple;
  RID rid;
  size_t n = this->plan_->GetN();
  while (this->child_executor_->Next(&tuple, &rid)) {
    auto entry = std::make_pair(std::move(tuple), rid);
    if (top_n_pq.size() < n) {
      top_n_pq.emplace(std::move(entry));
    } else {
      if (top_n_cmp(entry, top_n_pq.top())) {
        top_n_pq.pop();
        top_n_pq.emplace(std::move(entry));
      }
    }
  }

  this->top_n_result_.reserve(n);

  while (!top_n_pq.empty()) {
    this->top_n_result_.emplace_back(top_n_pq.top());
    top_n_pq.pop();
  }

  std::reverse(this->top_n_result_.begin(), this->top_n_result_.end());
  this->iter_ = this->top_n_result_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (this->iter_ != this->top_n_result_.end()) {
    std::tie(*tuple, *rid) = *this->iter_;
    ++this->iter_;
    return true;
  }
  return false;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return this->top_n_result_.size(); };

}  // namespace bustub
