#include <execution/plans/limit_plan.h>

#include "execution/executors/sort_executor.h"
#include "execution/executors/topn_executor.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetChildren().size() != 1) {
    return optimized_plan;
  }
  AbstractPlanNodeRef child = optimized_plan->GetChildren().at(0);
  if (optimized_plan->GetType() != PlanType::Limit || child->GetType() != PlanType::Sort) {
    return optimized_plan;
  }

  TopNPlanNode new_plan = TopNPlanNode(
      child->output_schema_, child->GetChildren().at(0),
      std::dynamic_pointer_cast<const SortPlanNode>(child)->GetOrderBy(),
      std::dynamic_pointer_cast<const LimitPlanNode>(std::shared_ptr<const AbstractPlanNode>(std::move(optimized_plan)))
          ->GetLimit());
  return std::static_pointer_cast<const AbstractPlanNode>(std::make_shared<TopNPlanNode>(new_plan));
}

}  // namespace bustub
