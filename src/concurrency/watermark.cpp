#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  if (current_reads_.empty()) {
    watermark_ = read_ts;
  }
  current_reads_[read_ts]++;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    current_reads_.erase(read_ts);
  }
  if (current_reads_.empty()) {
    return;
  }
  while (current_reads_.count(watermark_) == 0) {
    watermark_++;
  }
}

}  // namespace bustub
