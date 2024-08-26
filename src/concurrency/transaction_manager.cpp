//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  // 读锁，多读单写
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  // 获取当前事务 id
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  // 将当前事务放入全局 map 中
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // TODO(fall2023): set the timestamps here. Watermark updated below.
  // 当前事务的 read timestamp 为最近提交的 commit timestamp
  txn_ref->read_ts_ = running_txns_.commit_ts_;

  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  // TODO(fall2023): acquire commit ts!
  auto commit_ts = ++last_commit_ts_;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // TODO(fall2023): Implement the commit logic!
  // 提交时将事务修改的base的ts设置为较小数
  auto write_sets = txn->GetWriteSets();
  for (auto const &rid_set : write_sets) {
    for (auto const &rid : rid_set.second) {
      auto table_heap = catalog_->GetTable(rid_set.first)->table_.get();
      std::pair<TupleMeta, Tuple> base_pair = table_heap->GetTuple(rid);
      base_pair.first.ts_ = commit_ts;
      catalog_->GetTable(rid_set.first)->table_->UpdateTupleInPlace(base_pair.first, base_pair.second, rid);
    }
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;
  // 移除此事务开始时间戳意味着事务结束了，所以需要更新 commit_ts_
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() { UNIMPLEMENTED("not implemented"); }

}  // namespace bustub
