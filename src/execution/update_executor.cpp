//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  child_executor_ = std::move(child_executor);
}

void UpdateExecutor::Init() {
  has_updated_ = false;
  child_executor_->Init();
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_updated_) {
    return false;
  }
  has_updated_ = true;

  int count = 0;
  auto schema = table_info_->schema_;
  auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  Tuple old_tuple{};
  RID old_rid{};
  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 试图找到此条记录，若未找到，则不更新也不计数
    auto meta = table_info_->table_->GetTupleMeta(old_rid);
    if (meta.is_deleted_) {
      continue;
    }
    // 找到后将 meta 标记为删除
    table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, old_rid);

    // 计算新tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto &target_expression : plan_->target_expressions_) {
      values.push_back(target_expression->Evaluate(&old_tuple, schema));
    }
    Tuple new_tuple = Tuple(values, &schema);

    // 插入新纪录
    std::optional<RID> new_rid_optional = table_info_->table_->InsertTuple(TupleMeta{0, false}, new_tuple);
    RID new_rid = new_rid_optional.value();

    // 更新索引
    for (auto &index_info : indices) {
      auto old_key = old_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, new_rid, exec_ctx_->GetTransaction());
    }
    ++count;
  }

  // 返回答案
  std::vector<Value> result = {{TypeId::INTEGER, count}};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
