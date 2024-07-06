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
  has_scaned_ = false;
  // index_info_ 存了这个节点的索引信息，即这个节点通过本索引来找数据
  index_info_ = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  // htable_ 存了这个索引的扩展哈希表，通过这个来找索引对应的 RID
  htable_ = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
}

// 利用索引快速查找
auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(has_scaned_) {
    return false;
  }
  has_scaned_ = true;
return false;
/*
  // 首先，找到目标的 RID
  // 将 plan 中的 pred_key_ 重新组装，获取用于查找的 key
  auto key_index = Tuple(std::vector<Value>{plan_->pred_key_->val_}, &index_info_->key_schema_);
  std::vector<RID> result;
  htable_->ScanKey(key_index, &result, exec_ctx_->GetTransaction());

  // 找到 RID 后，找出对应的行的数据
  if(result.empty()) {
    return false;
  }
  *rid = *result.begin();
  auto meta_and_tuple = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_)->table_->GetTuple(*rid);
  if(meta_and_tuple.first.is_deleted_) {
    std::cout<<"deleted!!!"<<std::endl;
    return false;
  }
  *tuple = meta_and_tuple.second;
  return true;
*/
}

}  // namespace bustub
