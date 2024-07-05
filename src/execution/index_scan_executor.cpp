//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_info_(nullptr), htable_(nullptr) {}

void IndexScanExecutor::Init() {
  std::cout<<"here>>>"<<std::endl;
  index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 首先，找到目标的RID
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  auto schema = table_info->schema_;
  auto key = tuple->KeyFromTuple(schema, index_info_->key_schema_, index_info_->index_->GetKeyAttrs());
  std::vector<RID> result;
  htable_->ScanKey(key, &result, exec_ctx_->GetTransaction());
  std::cout<<"result: "<<result[0]<<std::endl;
  return true;
}

}  // namespace bustub
