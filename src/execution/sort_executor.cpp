#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      schema_(plan_->OutputSchema()) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::vector<Tuple> tuples;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.emplace_back(std::move(tuple));
  }
  std::sort(tuples.begin(), tuples.end(),
            [this](const Tuple &left, const Tuple &right) { return this->Cmp(left, right); });
  for (auto &t : tuples) {
    tuples_.push(t);
  }
}

auto SortExecutor::Cmp(const Tuple &left_tuple, const Tuple &right_tuple) const -> bool {
  for (auto &cond : plan_->GetOrderBy()) {
    OrderByType order_by = cond.first;
    AbstractExpressionRef abstract_expression = cond.second;
    Value left_value = abstract_expression->Evaluate(&left_tuple, schema_);
    Value right_value = abstract_expression->Evaluate(&right_tuple, schema_);
    if (left_value.CompareExactlyEquals(right_value)) {
      continue;
    }
    return ((order_by == OrderByType::ASC || order_by == OrderByType::DEFAULT) &&
            left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) ||
           (order_by == OrderByType::DESC && left_value.CompareLessThan(right_value) == CmpBool::CmpFalse);
  }
  return true;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.front();
  *rid = tuples_.front().GetRid();
  tuples_.pop();
  return true;
}

}  // namespace bustub
