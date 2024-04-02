#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_) {
      if (auto compare_expr = std::dynamic_pointer_cast<ComparisonExpression>(seq_scan_plan.filter_predicate_);
          compare_expr && compare_expr->comp_type_ == ComparisonType::Equal) {
        BUSTUB_ASSERT(compare_expr->children_.size() == 2, "must have exactly two children");

        const auto &left_expr = std::dynamic_pointer_cast<ColumnValueExpression>(compare_expr->GetChildAt(0));
        const auto &right_expr = std::dynamic_pointer_cast<ConstantValueExpression>(compare_expr->GetChildAt(1));
        if (left_expr && right_expr) {
          const auto col_idx = left_expr->GetColIdx();
          const auto *table_info = this->catalog_.GetTable(seq_scan_plan.GetTableOid());
          const auto indexs = this->catalog_.GetTableIndexes(table_info->name_);

          for (const auto *index : indexs) {
            const auto &columns = index->index_->GetKeyAttrs();
            if (std::find(columns.begin(), columns.end(), col_idx) != columns.end()) {
              return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, table_info->oid_,
                                                         index->index_oid_, nullptr, right_expr.get());
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
