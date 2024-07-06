#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

// 优化 SeqScan 成 IndexScan 的函数
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // 对每个子结点递归地使用 OptimizeMaergeFilterScan 后更新子节点。
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  // 得到子节点已被更新的当前节点
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 判断当前节点是否是 SeqScan 节点，若不是直接返回
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }
  const auto &seq_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  // 判断是否存在筛选条件，若不存在直接返回，predicate 类型：AbstractExpressionRef
  auto predicate = seq_plan.filter_predicate_;
  if (!predicate) {
    return optimized_plan;
  }
  // 判断是否是 v1=1 型谓词，若不是直接返回
  auto comp_expr = std::dynamic_pointer_cast<ComparisonExpression>(predicate);
  if (!comp_expr || comp_expr->comp_type_ != ComparisonType::Equal) {
    return optimized_plan;
  }
  auto pred_children = predicate->GetChildren();
  BUSTUB_ASSERT(pred_children.size() == 2, "must have exactly two children");
  if (!std::dynamic_pointer_cast<ColumnValueExpression>(pred_children[0]) ||
      !std::dynamic_pointer_cast<ConstantValueExpression>(pred_children[1])) {
    return optimized_plan;
  }
  // 判断列是否是索引，若是则进行优化
  auto col_idx = std::dynamic_pointer_cast<ColumnValueExpression>(pred_children[0])->GetColIdx();
  auto column = catalog_.GetTable(seq_plan.table_name_)->schema_.GetColumn(col_idx);
  auto indexes = catalog_.GetTableIndexes(seq_plan.table_name_); // std::vector<IndexInfo *>
  for(auto *index_info : indexes) {
    auto schema = index_info->index_->GetKeySchema();
    if(schema->GetColumnCount() != 1){
        continue;
    }
    if(schema->TryGetColIdx(column.GetName()) != std::nullopt) {
        auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(pred_children[1]);
        ConstantValueExpression *raw_pred_key = pred_key ? pred_key.get() : nullptr;
        return std::make_shared<IndexScanPlanNode>(seq_plan.output_schema_, seq_plan.GetTableOid(), index_info->index_oid_, predicate, raw_pred_key);// (TODO)返回优化后的节点！！！
    }
  }

  return optimized_plan;
}

}  // namespace bustub
