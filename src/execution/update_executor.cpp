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
    while(child_executor_->Next(tuple, rid)) {
        // TODO 这里不能直接拿 tuple 当目标插入
        std::cout<<tuple->ToString(&schema)<<std::endl;

        // 试图找到此条记录，若未找到，则不更新也不计数
        auto meta = table_info_->table_->GetTupleMeta(*rid);
        if(meta.is_deleted_) {
            continue;
        }
        // 找到后将 meta 标记为删除
        table_info_->table_->UpdateTupleMeta(TupleMeta{0, true}, *rid);
        // 标记后插入新纪录
        std::optional<RID> new_rid_optional = table_info_->table_->InsertTuple(TupleMeta{0, false}, *tuple);
        RID new_rid = new_rid_optional.value();
        // 更新索引
        for (auto &index_info : indices) {
            auto key = tuple->KeyFromTuple(schema, index_info->key_schema_, index_info->index_->GetKeyAttrs());
            index_info->index_->InsertEntry(key, new_rid, exec_ctx_->GetTransaction());
        }
        ++count;
    }

    // 返回答案
    std::vector<Value> result = {{TypeId::INTEGER, count}};
    *tuple = Tuple(result, &GetOutputSchema());
    return true;
}

}  // namespace bustub
