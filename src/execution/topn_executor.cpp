#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  if (has_executed_) {
    return;
  }
  has_executed_ = true;
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  std::priority_queue<Tuple, std::vector<Tuple>, Cmp> queue((Cmp(plan_)));
  while (child_executor_->Next(&tuple, &rid)) {
    queue.push(tuple);
    if (queue.size() > plan_->GetN()) {
      queue.pop();
    }
  }
  while (!queue.empty()) {
    tuples_.push(queue.top());
    queue.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (tuples_.empty()) {
    return false;
  }
  *tuple = tuples_.top();
  *rid = tuple->GetRid();
  tuples_.pop();
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return tuples_.size(); };
}  // namespace bustub
