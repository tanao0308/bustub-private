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

#include <concurrency/transaction_manager.h>

#include "common/exception.h"
#include "execution/execution_common.h"

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
  auto txn = exec_ctx_->GetTransaction()->GetTransactionId();
  std::vector<RID> buffer;
  while (child_executor_->Next(tuple, rid)) {
    // 试图找到此条记录，若未找到，则不更新也不计数
    std::pair<TupleMeta, Tuple> base_pair = table_info_->table_->GetTuple(*rid);
    if (base_pair.first.is_deleted_) {
      continue;
    }
    // 如果当前记录已被其他未结束的事务修改过，则抛出异常
    if (base_pair.first.ts_ >= TXN_START_ID && base_pair.first.ts_ != txn) {
      exec_ctx_->GetTransaction()->SetTainted();
      throw ExecutionException("write-write conflict: updated");
    }
    buffer.emplace_back(*rid);
  }
  for (auto old_rid : buffer) {
    // 计算新 tuple
    std::vector<Value> values;
    values.reserve(GetOutputSchema().GetColumnCount());
    for (auto &target_expression : plan_->target_expressions_) {
      values.push_back(target_expression->Evaluate(tuple, schema));
    }
    Tuple new_tuple = Tuple(values, &schema);
    // 修改真实数据
    auto old_base_pair = table_info_->table_->GetTuple(old_rid);
    table_info_->table_->UpdateTupleInPlace(TupleMeta{txn, false}, new_tuple, old_rid);
    // 更新索引
    for (auto &index_info : indices) {
      auto old_key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      auto new_key = new_tuple.KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
      index_info->index_->InsertEntry(new_key, old_rid, exec_ctx_->GetTransaction());
    }
    ++count;

    // TODO(tanao): 接下来为 P4 任务
    // 判断此记录之前是否存在过，若之前存在，则直接在上面改
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
      if (undo_log.is_deleted_) {
        undo_log.modified_fields_.assign(schema.GetColumnCount(), false);
      } else {
        old_old_tuple = ReconstructTuple(&schema, old_base_pair.second, old_base_pair.first, {undo_log}).value();
        GenerateUndolog(schema, old_old_tuple, new_tuple, undo_log.modified_fields_, undo_log.tuple_);
      }
      exec_ctx_->GetTransaction()->ModifyUndoLog(log_idx, undo_log);
      // 无需修改 undo_link
    }
    // 若之前不存在，则新建 undo_log 和 undo_link
    else {
      auto prev_undo_link_optional = exec_ctx_->GetTransactionManager()->GetUndoLink(old_rid);
      UndoLog undo_log;
      undo_log.is_deleted_ = false;
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
  }
  // 返回答案
  *tuple = Tuple({Value(TypeId::INTEGER, count)}, &GetOutputSchema());
  return true;
}

// 此函数作用：给出从 new_tuple 到 old_tuple 的变换掩码和值: modified_fields, tuple
// 只能对付 old new 都非删除的情况
void UpdateExecutor::GenerateUndolog(const Schema &schema, const Tuple &old_tuple, const Tuple &new_tuple,
                                     std::vector<bool> &modified_fields, Tuple &tuple) {
  // modified_fields.clear();
  std::vector<Value> values;
  std::vector<Column> columns;
  for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
    Value old_value = old_tuple.GetValue(&schema, i);
    Value new_value = new_tuple.GetValue(&schema, i);
    // 后一句是为了 "we ask you to keep the undo log unchanged – only adding more data to undo log but not removing
    // data,
    //             so as to make it easier to handle concurrency issues."
    if (old_value.CompareExactlyEquals(new_value) && !modified_fields.at(i)) {
      continue;
    }
    modified_fields.at(i) = true;
    values.push_back(old_value);
    columns.push_back(schema.GetColumn(i));
  }
  Schema temp_schema(columns);
  tuple = Tuple(values, &temp_schema);
}

}  // namespace bustub
