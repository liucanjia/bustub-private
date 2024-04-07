#include "execution/executors/window_function_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  this->child_executor_->Init();
  this->cnt_ = 0;
  this->result_.clear();

  std::vector<Tuple> origin_tuples;

  Tuple tuple;
  RID rid;
  auto schema = this->child_executor_->GetOutputSchema();
  // read the tuple in memory
  while (this->child_executor_->Next(&tuple, &rid)) {
    origin_tuples.emplace_back(std::move(tuple));
  }
  // if window_functions has order_by, sort the tuple firstly
  // assume all order_by is same
  for (const auto &[idx, window_fun] : this->plan_->window_functions_) {
    if (!window_fun.order_by_.empty()) {
      auto sort_cmp = [&, window_fun = window_fun](const Tuple &a, const Tuple &b) -> bool {
        for (const auto &[orderby_type, expr] : window_fun.order_by_) {
          auto a_col_value = expr->Evaluate(&a, schema);
          auto b_col_value = expr->Evaluate(&b, schema);
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

      std::sort(origin_tuples.begin(), origin_tuples.end(), sort_cmp);
      break;
    }
  }

  size_t cols_cnt = this->plan_->columns_.size();
  this->result_.reserve(cols_cnt);
  for (size_t i = 0; i < cols_cnt; ++i) {
    this->result_.emplace_back(std::vector<Value>(origin_tuples.size()));
  }

  // read the column idx that is not placeholder
  for (size_t i = 0; i < cols_cnt; ++i) {
    auto col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(this->plan_->columns_[i]);
    auto col_idx = col_expr->GetColIdx();
    if (col_idx != 0xffffffff) {  // if col_idx == -1, mean placeholder
      for (size_t j = 0; j < origin_tuples.size(); ++j) {
        this->result_[i][j] = col_expr->Evaluate(&origin_tuples[j], schema);
      }
    }
  }

  for (const auto &[idx, window_fun] : this->plan_->window_functions_) {
    this->Helper(idx, window_fun, origin_tuples);
  }
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->cnt_ < this->result_[0].size()) {
    size_t n = this->result_.size();
    std::vector<Value> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; i++) {
      vals.emplace_back(this->result_[i][this->cnt_]);
    }

    *tuple = Tuple{vals, &this->plan_->OutputSchema()};
    ++this->cnt_;
    return true;
  }
  return false;
}

void WindowFunctionExecutor::Helper(size_t output_col_idx, const WindowFunctionPlanNode::WindowFunction &window_fun,
                                    std::vector<Tuple> &tuples) {
  this->ht_.clear();

  auto schema = this->child_executor_->GetOutputSchema();
  for (size_t idx = 0; idx < tuples.size(); idx++) {
    PartitionKey part_key = this->MakeParititionKey(tuples[idx], window_fun.partition_by_);
    if (auto it = this->ht_.find(part_key); it != this->ht_.end()) {
      it->second.emplace_back(idx);
    } else {
      this->ht_[part_key] = std::vector<size_t>{idx};
    }
  }

  bool order_by_flag = window_fun.order_by_.empty();
  for (const auto &[key, idxes] : this->ht_) {
    switch (window_fun.type_) {
      case WindowFunctionType::CountStarAggregate:
        if (order_by_flag) {
          size_t n = idxes.size();
          for (const auto &idx : idxes) {
            this->result_[output_col_idx][idx] = ValueFactory::GetIntegerValue(n);
          }
        } else {
          for (size_t i = 0; i < idxes.size(); ++i) {
            this->result_[output_col_idx][idxes[i]] = ValueFactory::GetIntegerValue(i + 1);
          }
        }
        break;
      case WindowFunctionType::CountAggregate: {
        auto col_idx = std::dynamic_pointer_cast<ColumnValueExpression>(window_fun.function_)->GetColIdx();
        if (order_by_flag) {
          size_t n = 0;
          for (const auto &idx : idxes) {
            if (!tuples[idx].GetValue(&schema, col_idx).IsNull()) {
              n++;
            }
          }
          for (const auto &idx : idxes) {
            if (n != 0) {
              this->result_[output_col_idx][idx] = ValueFactory::GetIntegerValue(n);
            } else {
              this->result_[output_col_idx][idx] = ValueFactory::GetNullValueByType(TypeId::INTEGER);
            }
          }
        } else {
          size_t n = 0;
          for (const auto &idx : idxes) {
            if (!tuples[idx].GetValue(&schema, col_idx).IsNull()) {
              n++;
            }

            if (n != 0) {
              this->result_[output_col_idx][idx] = ValueFactory::GetIntegerValue(n);
            } else {
              this->result_[output_col_idx][idx] = ValueFactory::GetNullValueByType(TypeId::INTEGER);
            }
          }
        }
        break;
      }
      case WindowFunctionType::SumAggregate: {
        auto col_idx = std::dynamic_pointer_cast<ColumnValueExpression>(window_fun.function_)->GetColIdx();
        Value sum = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        if (order_by_flag) {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (sum.IsNull()) {
                sum = val;
              } else {
                sum = sum.Add(val);
              }
            }
          }
          for (const auto &idx : idxes) {
            this->result_[output_col_idx][idx] = sum;
          }
        } else {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (sum.IsNull()) {
                sum = val;
              } else {
                sum = sum.Add(val);
              }
            }
            this->result_[output_col_idx][idx] = sum;
          }
        }
        break;
      }
      case WindowFunctionType::MinAggregate: {
        auto col_idx = std::dynamic_pointer_cast<ColumnValueExpression>(window_fun.function_)->GetColIdx();
        Value min_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        if (order_by_flag) {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (min_value.IsNull() || min_value.CompareGreaterThan(val) == CmpBool::CmpTrue) {
                min_value = val;
              }
            }
          }

          for (const auto &idx : idxes) {
            this->result_[output_col_idx][idx] = min_value;
          }
        } else {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (min_value.IsNull() || min_value.CompareGreaterThan(val) == CmpBool::CmpTrue) {
                min_value = val;
              }
            }
            this->result_[output_col_idx][idx] = min_value;
          }
        }
        break;
      }
      case WindowFunctionType::MaxAggregate: {
        auto col_idx = std::dynamic_pointer_cast<ColumnValueExpression>(window_fun.function_)->GetColIdx();
        Value max_value = ValueFactory::GetNullValueByType(TypeId::INTEGER);
        if (order_by_flag) {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (max_value.IsNull() || max_value.CompareLessThan(val) == CmpBool::CmpTrue) {
                max_value = val;
              }
            }
          }

          for (const auto &idx : idxes) {
            this->result_[output_col_idx][idx] = max_value;
          }
        } else {
          for (const auto &idx : idxes) {
            if (auto val = tuples[idx].GetValue(&schema, col_idx); !val.IsNull()) {
              if (max_value.IsNull() || max_value.CompareLessThan(val) == CmpBool::CmpTrue) {
                max_value = val;
              }
            }
            this->result_[output_col_idx][idx] = max_value;
          }
        }
        break;
      }
      case WindowFunctionType::Rank:
        if (order_by_flag) {
          throw ExecutionException("Ensures that ORDER BY clause is not empty if RANK window function is present!");
        }
        OrderKey rank_orderby_val = this->MakeOrderKey(tuples[idxes[0]], window_fun.order_by_);
        this->result_[output_col_idx][idxes[0]] = ValueFactory::GetIntegerValue(1);
        for (size_t i = 1; i < idxes.size(); ++i) {
          OrderKey cur_orderby_val = this->MakeOrderKey(tuples[idxes[i]], window_fun.order_by_);
          if (cur_orderby_val == rank_orderby_val) {
            this->result_[output_col_idx][idxes[i]] = this->result_[output_col_idx][idxes[i - 1]];
          } else {
            this->result_[output_col_idx][idxes[i]] = ValueFactory::GetIntegerValue(i + 1);
            rank_orderby_val = cur_orderby_val;
          }
        }
        break;
    }
  }
}

auto WindowFunctionExecutor::MakeOrderKey(const Tuple &tuple,
                                          const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &orderby)
    -> OrderKey {
  std::vector<Value> keys;
  keys.reserve(orderby.size());
  for (const auto &[type, expr] : orderby) {
    keys.emplace_back(expr->Evaluate(&tuple, this->child_executor_->GetOutputSchema()));
  }
  return {keys};
}

auto WindowFunctionExecutor::MakeParititionKey(const Tuple &tuple,
                                               const std::vector<AbstractExpressionRef> &partitionby) -> PartitionKey {
  std::vector<Value> keys;
  keys.reserve(partitionby.size());
  for (const auto &expr : partitionby) {
    keys.emplace_back(expr->Evaluate(&tuple, this->child_executor_->GetOutputSchema()));
  }
  return {keys};
}
}  // namespace bustub
