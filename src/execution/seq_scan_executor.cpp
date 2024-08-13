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

#include <concurrency/transaction_manager.h>

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr) {}

void SeqScanExecutor::Init() {
  table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // TupleMeta meta;
  std::optional<Tuple> opt_tuple;
  do {
    if (iter_->IsEnd()) {
      return false;
    }
    opt_tuple = PassVersion(rid);
    // meta = iter_->GetTuple().first;
    // if (!meta.is_deleted_) {
    //   *tuple = iter_->GetTuple().second;
    //   *rid = iter_->GetRID();
    // }
    ++(*iter_);
  } while (opt_tuple == std::nullopt || PassFilter(&opt_tuple.value()));
  return true;
}

auto SeqScanExecutor::PassFilter(Tuple *tuple) -> bool {
  return plan_->filter_predicate_ != nullptr &&
         !plan_->filter_predicate_
              ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
              .GetAs<bool>();
}

auto SeqScanExecutor::PassVersion(RID *rid) -> std::optional<Tuple> {
  Transaction *transaction = exec_ctx_->GetTransaction();
  transaction->GetUndoLog(); // 获取当前事务的某条 undolog
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  std::optional<UndoLink> undoLink = txn_mgr->GetUndoLink(*rid);
  if(!undoLink.has_value()) {
    return std::nullopt; // TODO 返回什么值
  }
  txn_id_t prev_txn = undoLink->prev_txn_;

  // 处理情况二，上次修改是本事务进行的修改
  for(size_t i=transaction->GetUndoLogNum()-1; i!=0; ++i) {
    UndoLog undo_log = transaction->GetUndoLog(i);
    if(undo_log.tuple_.GetRid() == *rid) {
      return undo_log.is_deleted_ ? std::nullopt : undo_log.tuple_;
    }
  }
  // 处理情况一，上一个提交的事务，结束时间戳 <= 本事务开始时间戳
  if(txn_mgr->txn_map_.at(prev_txn)->GetCommitTs() <= transaction->GetReadTs()) {
    std::pair<TupleMeta, Tuple> temp_pair = table_heap_->GetTuple(*rid);
    return temp_pair.first.is_deleted_ ? std::nullopt : temp_pair.second;
  }

  transaction->GetReadTs(); // 当前事务的开始时间戳
  // 1. 当前记录是被此事务修改过的


  return std::nullopt;
}

}  // namespace bustub
