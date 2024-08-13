#pragma once

#include <unordered_map>

#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * @brief tracks all the read timestamps.
 *
 */
class Watermark {
 public:
  explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

  auto AddTxn(timestamp_t read_ts) -> void;

  auto RemoveTxn(timestamp_t read_ts) -> void;

  /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
   * correctly. */
  // 移除此事务开始时间戳意味着事务结束了，所以需要更新 commit_ts_
  auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

  auto GetWatermark() -> timestamp_t {
    if (current_reads_.empty()) {
      return commit_ts_;
    }
    return watermark_;
  }

  // 当前提交时间戳，它用于跟踪系统中最新的提交时间戳。应该和外界的 last_commit_ts_ 相等
  timestamp_t commit_ts_;

  // 当前的水印时间戳。它表示了系统中所有正在执行的事务的最小开始时间戳。如果没有事务正在运行，则设置为最后的事务的提交时间戳。
  timestamp_t watermark_;

  // 用于记录所有当前活跃的读取时间戳（事务开始时间戳），以及每个时间戳被读取的次数
  std::unordered_map<timestamp_t, int> current_reads_;
};

};  // namespace bustub
