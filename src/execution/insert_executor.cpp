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

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(child_executor) {}

void InsertExecutor::Init() {
    
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
    auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
    auto schema = ;
    auto indices = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

    while(child_executor_->Next(tuple, rid)) {
        // 增加答案
        ++count;
        // 插入记录
        for(auto &index_info : indices) {
            
        }
    }
    // 这里的 tuple 不再对应实际的数据行，而是用来存储插入操作的影响行数
    *tuple = Tuple(count, &GetOutputSchema());
    return true;
}

}  // namespace bustub
