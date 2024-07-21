//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  child_executor_->Init();
  has_deleted_ = false;
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (has_deleted_) {
    return false;
  }
  has_deleted_ = true;

  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    // 试图找到此条记录，若未找到，则不更新也不计数
    auto meta = table_info->table_->GetTupleMeta(*rid);
    if (meta.is_deleted_) {
      continue;
    }
    // 找到后将 meta 标记为删除
    table_info->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
    // 更新索引
    for (auto &index_info : indexes) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }

  // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
  std::vector<Value> result = {Value(TypeId::INTEGER, count)};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
