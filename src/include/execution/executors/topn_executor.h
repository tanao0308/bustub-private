//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

#include <stack>

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The TopN plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the TopN */
  void Init() override;

  /**
   * Yield the next tuple from the TopN.
   * @param[out] tuple The next tuple produced by the TopN
   * @param[out] rid The next tuple RID produced by the TopN
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the TopN */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

 private:
  /** 定义堆的比较结构体 */
  struct Cmp {
    const TopNPlanNode *plan_;

    explicit Cmp(const TopNPlanNode *plan) : plan_(plan) {}

    auto operator()(const Tuple &left_tuple, const Tuple &right_tuple) const -> bool {
      Schema schema = plan_->OutputSchema();
      for (auto &cond : plan_->GetOrderBy()) {
        OrderByType order_by = cond.first;
        AbstractExpressionRef abstract_expression = cond.second;
        Value left_value = abstract_expression->Evaluate(&left_tuple, schema);
        Value right_value = abstract_expression->Evaluate(&right_tuple, schema);
        if (left_value.CompareExactlyEquals(right_value)) {
          continue;
        }
        return ((order_by == OrderByType::ASC || order_by == OrderByType::DEFAULT) &&
                left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) ||
               (order_by == OrderByType::DESC && left_value.CompareLessThan(right_value) == CmpBool::CmpFalse);
      }
      return true;
    }
  };

  /** The TopN plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  // 防止重复 init
  bool has_executed_{false};
  /** 倒序堆，以 Cmp 逆序排序，这样可以保留最大元素 */
  std::stack<Tuple> tuples_;
};
}  // namespace bustub
