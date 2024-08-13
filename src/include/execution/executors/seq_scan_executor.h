//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  // 每个执行器需要初始化的东西都不一样，一般是初始化指向表头的迭代器指针，或者自己定义的一些辅助循环的值
  // 拿seq_scan来说，就需要在这里将迭代器指针指向表头。
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  // 需要做到调用一次next就输出且只输出一组tuple的功能，输出是通过赋值*tuple参数，并return true
  // 如果没有能输出的了，return false
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  // Schema相当于存储了表的表头名字，OutputSchema用来指出此节点需要输出哪些列（哪些表头需要考虑在内）
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;

  TableHeap *table_heap_{nullptr};
  std::unique_ptr<TableIterator> iter_;

  auto PassFilter(Tuple *tuple) -> bool;

  auto PassVersion(RID *rid) -> std::optional<Tuple>;
};
}  // namespace bustub
