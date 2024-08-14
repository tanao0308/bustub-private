#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if ((!undo_logs.empty() && undo_logs.back().is_deleted_) || (undo_logs.empty() && base_meta.is_deleted_)) {
    return std::nullopt;
  }
  std::vector<Value> data;
  for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
    data.push_back(base_tuple.GetValue(schema, i));
  }

  for (auto undo_log : undo_logs) {
    std::vector<uint32_t> attrs;
    for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (undo_log.modified_fields_.at(i)) {
        attrs.push_back(i);
      }
    }
    Schema temp_schema = Schema::CopySchema(schema, attrs);
    int cnt = 0;
    for (size_t i = 0; i < schema->GetColumnCount(); ++i) {
      if (!undo_log.modified_fields_.at(i)) {
        continue;
      }
      data.at(i) = undo_log.tuple_.GetValue(&temp_schema, cnt);
      cnt++;
    }
  }
  return Tuple(data, schema);
}

auto GetSchema(Tuple raw_tuple, std::vector<bool> modified_fields, Schema schema)->Schema {
  std::vector<Column> columns;
  for(size_t i=0;i<schema.GetColumnCount();++i) {
    if(modified_fields.at(i)) {
      columns.push_back(schema.GetColumn(i));
    }
  }
  return Schema(columns);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::cout<<">>>>>>>>>>>>>>>>>>>>>"<<std::endl;
  fmt::println(stderr, "debug_hook: {}", info);
  std::cout<<"table name: "<<table_info->name_<<std::endl;
  std::cout<<"table schema: "<<table_info->schema_.ToString()<<std::endl;
  TableIterator iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    ++iter;
    std::pair<TupleMeta, Tuple> base_pair = table_heap->GetTuple(rid);
    // TODO
    std::cout<<"\tRID="<<rid.GetPageId()<<"/"<<rid.GetSlotNum()<<" ts="<<(base_pair.first.ts_>=TXN_START_ID?base_pair.first.ts_-TXN_START_ID:base_pair.first.ts_)<<" is_delete="<<base_pair.first.is_deleted_
      <<" tuple="<<base_pair.second.ToString(&table_info->schema_)<<std::endl;

    UndoLink undo_link = txn_mgr->GetUndoLink(rid).value();
    if(!undo_link.IsValid()) {
      continue;
    }
    UndoLog undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
    while(true) {
      Schema temp_schema = GetSchema(undo_log.tuple_, undo_log.modified_fields_, table_info->schema_);

      std::cout<<"\t\ttxn"<<undo_link.prev_txn_<<" is_delete="<<undo_log.is_deleted_<<" ts="<<undo_log.ts_
        <<" tuple="<<undo_log.tuple_.ToString(&temp_schema)<<std::endl;
      undo_link = undo_log.prev_version_;
      if(!undo_link.IsValid()) {
        break;
      }
      undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
    }
  }
  std::cout<<">>>>>>>>>>>>>>>>>>>>>"<<std::endl;
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
