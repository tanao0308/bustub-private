//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr) {}

void SeqScanExecutor::Init() {
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  TupleMeta meta;
  do {
    if (iter_->IsEnd()) {
      return false;
    }
    meta = iter_->GetTuple().first;
    if (!meta.is_deleted_) {
      *tuple = iter_->GetTuple().second;
      *rid = iter_->GetRID();
    }
    ++(*iter_);
  } while (meta.is_deleted_ || PassFilter(tuple));
  return true;
}

auto SeqScanExecutor::PassFilter(Tuple *tuple) -> bool {
  return plan_->filter_predicate_ != nullptr &&
         !plan_->filter_predicate_
              ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
              .GetAs<bool>();
}

}  // namespace bustub
