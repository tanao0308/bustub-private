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

#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"

#include <concurrency/transaction_manager.h>

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
  auto txn = exec_ctx_->GetTransaction()->GetTransactionId();

  std::vector<std::pair<Tuple, RID>> buffer;
  while (child_executor_->Next(tuple, rid)) {
    // 试图找到此条记录，若未找到，则不更新也不计数
    std::pair<TupleMeta, Tuple> base_pair = table_info->table_->GetTuple(*rid);
    // 如果当前记录 1.正在被另一个事务修改 2.已被比自己晚开始且已提交的事务删除 则抛出异常
    if ((base_pair.first.ts_ >= TXN_START_ID || base_pair.first.ts_ > exec_ctx_->GetTransaction()->GetReadTs()) &&
        base_pair.first.ts_ != txn) {
      exec_ctx_->GetTransaction()->SetTainted();
      std::cout << "!!!!!!!" << std::endl;
      throw ExecutionException("write-write conflict: deleted");
    }
    if (base_pair.first.is_deleted_) {
      continue;
    }
    buffer.emplace_back(*tuple, *rid);
  }
  for (auto const &pair : buffer) {
    Tuple old_tuple = pair.first;
    RID old_rid = pair.second;

    // 找到后将 meta 标记为删除
    auto old_base_pair = table_info->table_->GetTuple(old_rid);
    table_info->table_->UpdateTupleMeta(TupleMeta{txn, true}, old_rid);
    // 更新索引
    for (auto &index_info : indexes) {
      auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
    }
    ++count;

    // TODO(tanao): 接下来为 P4 内容
    std::vector<Value> values;
    for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
      values.emplace_back(schema.GetColumn(i).GetType());
    }
    Tuple new_tuple = Tuple(values, &schema);  // 如何将 new 设置为全 null
    // 判断此记录当前事务是否之前修改过，若之前修改过，则直接在上面改
    if (old_base_pair.first.ts_ == txn) {
      auto prev_undo_link_optional = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      // 此条修改的是之前没 undo_log 的条目，则改数据即可
      if (!prev_undo_link_optional.has_value() || !prev_undo_link_optional->IsValid()) {
        continue;
      }
      // 此次修改的是之前有 undo_log 的条目，则需要修改之前的 undo_log
      int log_idx = prev_undo_link_optional.value().prev_log_idx_;
      UndoLog undo_log = exec_ctx_->GetTransaction()->GetUndoLog(log_idx);
      Tuple old_old_tuple;
      // if (undo_log.is_deleted_) ? 不可能吧
      old_old_tuple = ReconstructTuple(&schema, old_base_pair.second, old_base_pair.first, {undo_log}).value();
      GenerateUndolog(schema, old_old_tuple, new_tuple, undo_log.modified_fields_, undo_log.tuple_);
      // undo_log.is_deleted_ = true;
      exec_ctx_->GetTransaction()->ModifyUndoLog(log_idx, undo_log);
      // 无需修改 undo_link
    }
    // 若之前不存在，则新建 undo_log 和 undo_link
    else {
      auto prev_undo_link_optional = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      UndoLog undo_log;
      undo_log.is_deleted_ = old_base_pair.first.is_deleted_;
      undo_log.ts_ = old_base_pair.first.ts_;
      if (prev_undo_link_optional.has_value()) {
        undo_log.prev_version_ = prev_undo_link_optional.value();
      }
      undo_log.modified_fields_.assign(schema.GetColumnCount(), false);
      GenerateUndolog(schema, old_base_pair.second, new_tuple, undo_log.modified_fields_, undo_log.tuple_);
      exec_ctx_->GetTransaction()->AppendUndoLog(undo_log);

      UndoLink undo_link;
      undo_link.prev_txn_ = txn;
      undo_link.prev_log_idx_ = exec_ctx_->GetTransaction()->GetUndoLogNum() - 1;
      exec_ctx_->GetTransactionManager()->UpdateUndoLink(old_rid, undo_link);
    }
    exec_ctx_->GetTransaction()->AppendWriteSet(plan_->GetTableOid(), old_rid);
  }

  // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
  std::vector<Value> result = {Value(TypeId::INTEGER, count)};
  *tuple = Tuple(result, &GetOutputSchema());
  return true;
}

}  // namespace bustub
