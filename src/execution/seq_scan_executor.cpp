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
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  std::optional<UndoLink> undoLink = txn_mgr->GetUndoLink(*rid);

  if(!undoLink.has_value()) {
    return std::nullopt;
  }
  UndoLog undo_log = txn_mgr->txn_map_.at(undoLink.value().prev_txn_)->GetUndoLog(undoLink.value().prev_txn_);
  // 如果是最新记录，则直接返回
  if(undo_log.ts_ < transaction->GetReadTs()) {
    return undo_log.tuple_;
  }
  // 如果是本次记录，则直接返回
  if(undo_log.ts_ == transaction)


  // >>>>>>>
  // 对于一条记录，有链式的 undolog
  UndoLink undoLink1 = txn_mgr->GetUndoLink(*rid).value();
  UndoLog undo_log1 = txn_mgr->txn_map_.at(undoLink1.prev_txn_)->GetUndoLog(undoLink1.prev_log_idx_);
  UndoLink undoLink2 = undo_log1.prev_version_;
  UndoLog undo_log2 = txn_mgr->txn_map_.at(undoLink2.prev_txn_)->GetUndoLog(undoLink2.prev_log_idx_);


  // <<<<<<<


  Schema schema = plan_->OutputSchema();
  std::pair<TupleMeta, Tuple> base_pair = table_heap_->GetTuple(*rid);
  TupleMeta base_meta = base_pair.first;
  Tuple base_tuple = base_pair.second;
  std::vector<UndoLog> undo_logs;

  for(size_t i=0; i<transaction->GetUndoLogNum(); ++i) {
    UndoLog undo_log = transaction->GetUndoLog(i);
    if(undo_log.tuple_.GetRid() == *rid) {
      undo_logs.push_back(undo_log);
    }
  }
  // 上次修改是本事务进行的修改 && 上一个提交的事务结束时间戳 <= 本事务开始时间戳
  if(!undo_logs.empty() ||
    (undo_logs.empty() && (prev_txn == TXN_START_ID - 1 ||txn_mgr->txn_map_.at(prev_txn)->GetCommitTs() <= transaction->GetReadTs()))) {
    return ReconstructTuple(&schema, base_tuple, base_meta, undo_logs);
  }
  // 上一个提交的事务开始时间戳 >


  return std::nullopt;
}

}  // namespace bustub
