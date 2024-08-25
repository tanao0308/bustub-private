//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

#include <concurrency/transaction_manager.h>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  child_executor_ = std::move(child_executor);
}

void InsertExecutor::Init() {
  has_inserted_ = false;
  child_executor_->Init();
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (has_inserted_) {
    return false;
  }
  has_inserted_ = true;

  int count = 0;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto schema = table_info->schema_;
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(tuple, rid)) {
    // 插入记录
    UndoLog undo_log;
    undo_log.is_deleted_ = true;
    undo_log.modified_fields_ = {};
    undo_log.ts_ = exec_ctx_->GetTransaction()->GetTransactionId();
    RID new_rid = table_info->table_->InsertTuple(TupleMeta{undo_log.ts_, false}, *tuple).value();
    exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);
    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), new_rid);
    // 更新索引
    for (auto &index_info : indexes) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
    }
    // 增加答案
    ++count;
  }
  // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
  return true;
}

}  // namespace bustub
