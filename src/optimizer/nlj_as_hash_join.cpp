#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Helper(const std::shared_ptr<ComparisonExpression> &comparison_expr,
            std::vector<AbstractExpressionRef> &left_key_expressions,
            std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  const auto &left_col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(0));
  const auto &right_col_expr = std::dynamic_pointer_cast<ColumnValueExpression>(comparison_expr->GetChildAt(1));

  if (left_col_expr && right_col_expr) {
    if (left_col_expr->GetTupleIdx() == 0) {
      left_key_expressions.emplace_back(left_col_expr);
    } else {
      right_key_expressions.emplace_back(
          std::make_shared<ColumnValueExpression>(0, left_col_expr->GetColIdx(), left_col_expr->GetReturnType()));
    }

    if (right_col_expr->GetTupleIdx() == 0) {
      left_key_expressions.emplace_back(right_col_expr);
    } else {
      right_key_expressions.emplace_back(
          std::make_shared<ColumnValueExpression>(0, right_col_expr->GetColIdx(), right_col_expr->GetReturnType()));
    }

    return true;
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nested_loop_join_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    auto expr = nested_loop_join_plan.Predicate();
    std::vector<AbstractExpressionRef> left_key_expressions;
    std::vector<AbstractExpressionRef> right_key_expressions;
    auto left_plan = nested_loop_join_plan.GetLeftPlan();
    auto right_plan = nested_loop_join_plan.GetRightPlan();
    JoinType join_type = nested_loop_join_plan.GetJoinType();

    if (RewriteExpressionForHashJoin(expr, left_key_expressions, right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nested_loop_join_plan.output_schema_, left_plan, right_plan,
                                                left_key_expressions, right_key_expressions, join_type);
    }
  }
  return optimized_plan;
}

auto Optimizer::RewriteExpressionForHashJoin(const AbstractExpressionRef &expr,
                                             std::vector<AbstractExpressionRef> &left_key_expressions,
                                             std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  if (!expr) {
    return false;
  }

  if (auto logic_expr = std::dynamic_pointer_cast<LogicExpression>(expr);
      logic_expr && logic_expr->logic_type_ == LogicType::And) {
    BUSTUB_ASSERT(logic_expr->children_.size() == 2, "must have exactly two children");

    const auto &left_expr = logic_expr->GetChildAt(0);
    const auto &right_exor = logic_expr->GetChildAt(1);
    if (std::dynamic_pointer_cast<LogicExpression>(left_expr) ||
        std::dynamic_pointer_cast<ComparisonExpression>(left_expr)) {
      RewriteExpressionForHashJoin(left_expr, left_key_expressions, right_key_expressions);
    } else {
      return false;
    }

    if (std::dynamic_pointer_cast<LogicExpression>(right_exor) ||
        std::dynamic_pointer_cast<ComparisonExpression>(right_exor)) {
      RewriteExpressionForHashJoin(right_exor, left_key_expressions, right_key_expressions);
    } else {
      return false;
    }

    return true;
  }

  if (auto comparison_expr = std::dynamic_pointer_cast<ComparisonExpression>(expr)) {
    BUSTUB_ASSERT(comparison_expr->children_.size() == 2, "must have exactly two children");
    return Helper(comparison_expr, left_key_expressions, right_key_expressions);
  }
  return false;
}

}  // namespace bustub
