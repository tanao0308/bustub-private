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
  std::optional<Tuple> opt_tuple;
  while (!iter_->IsEnd()) {
    // 计算出此事务能看到的 tuple
    opt_tuple = PassVersion(iter_->GetRID());
    ++(*iter_);

    // 若满足 tuple 存在且值通过筛选，则返回
    if (opt_tuple.has_value() && PassFilter(&opt_tuple.value())) {
      *tuple = opt_tuple.value();
      *rid = opt_tuple.value().GetRid();
      LOG_DEBUG("tuple=%s, rid=%s", tuple->ToString(&plan_->OutputSchema()).c_str(), rid->ToString().c_str());
      return true;
    }
  }
  return false;
}

auto SeqScanExecutor::PassFilter(Tuple *tuple) -> bool {
  return plan_->filter_predicate_ == nullptr ||
         plan_->filter_predicate_
             ->Evaluate(tuple, GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)
             .GetAs<bool>();
}

auto SeqScanExecutor::PassVersion(RID rid) -> std::optional<Tuple> {
  LOG_DEBUG("here");
  // 获取事务、事务管理器、本条数据的 undolog 链表头
  Transaction *transaction = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  if (!txn_mgr->GetUndoLink(rid).has_value()) {
    LOG_DEBUG("return 1");
    return std::nullopt;
  }
  std::pair<TupleMeta, Tuple> base_pair = table_heap_->GetTuple(rid);

  // 如果是最新记录或本次记录，则直接返回
  if (base_pair.first.ts_ <= transaction->GetReadTs() || base_pair.first.ts_ == transaction->GetTransactionId()) {
    LOG_DEBUG("return 2");
    return ReconstructTuple(&plan_->OutputSchema(), base_pair.second, base_pair.first, {});
  }

  UndoLink undo_link = txn_mgr->GetUndoLink(rid).value();
  UndoLog undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
  undo_link = undo_log.prev_version_;
  // 否则需要找到能读的版本，处理之后进行返回
  std::vector<UndoLog> undo_logs;
  undo_logs.push_back(undo_log);
  while (undo_log.ts_ > transaction->GetReadTs() && undo_log.ts_ != transaction->GetTransactionId()) {
    if (!undo_link.IsValid()) {
      // 这里不能 break 吗？why？
      LOG_DEBUG("return 3");
      return std::nullopt;
    }
    undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
    undo_link = undo_log.prev_version_;
    undo_logs.push_back(undo_log);
  }
  LOG_DEBUG("return 4");
  return ReconstructTuple(&plan_->OutputSchema(), base_pair.second, base_pair.first, undo_logs);
}

}  // namespace bustub
