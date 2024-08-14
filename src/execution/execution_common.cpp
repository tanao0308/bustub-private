#include "execution/execution_common.h"
#include <iomanip>
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

auto GetCompleteTuple(const Tuple &raw_tuple, std::vector<bool> modified_fields, const Schema &schema) -> std::string {
  std::stringstream os;
  bool first = true;
  os << "(";
  for (size_t i = 0, cnt = 0; i < schema.GetColumnCount(); i++) {
    if (first) {
      first = false;
    } else {
      os << ", ";
    }
    if (!modified_fields.at(i)) {
      os << "_";
      continue;
    }
    if (raw_tuple.IsNull(&schema, i)) {
      os << "<NULL>";
      continue;
    }
    Value val = (raw_tuple.GetValue(&schema, cnt++));
    os << val.ToString();
  }
  os << ")";

  return os.str();
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  std::cout << ">>>>>>>>>>>>>>>>>>>>>" << std::endl;
  fmt::println(stderr, "debug_hook: {}", info);
  std::cout << "table name: " << table_info->name_ << std::endl;
  std::cout << "table schema: " << table_info->schema_.ToString() << std::endl;
  TableIterator iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    ++iter;
    std::pair<TupleMeta, Tuple> base_pair = table_heap->GetTuple(rid);
    std::cout << "\t"
              << " RID=" << rid.GetPageId() << "/" << rid.GetSlotNum()
              << " ts=" << (base_pair.first.ts_ >= TXN_START_ID ? "txn" : "") << base_pair.first.ts_ % TXN_START_ID
              << " is_delete=" << base_pair.first.is_deleted_
              << " tuple=" << base_pair.second.ToString(&table_info->schema_) << std::endl;

    UndoLink undo_link = txn_mgr->GetUndoLink(rid).value();
    if (!undo_link.IsValid()) {
      continue;
    }
    UndoLog undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
    std::vector<UndoLog> undo_logs;
    while (true) {
      undo_logs.push_back(undo_log);
      std::optional<Tuple> temp_tuple =
          ReconstructTuple(&table_info->schema_, base_pair.second, base_pair.first, undo_logs);
      std::cout << "\t\t"
                << " ts=" << undo_log.ts_ << " is_delete=" << undo_log.is_deleted_ << " txn"
                << undo_link.prev_txn_ - TXN_START_ID
                << " tuple=" << GetCompleteTuple(undo_log.tuple_, undo_log.modified_fields_, table_info->schema_)
                << " true tuple=" << (temp_tuple.has_value() ? temp_tuple->ToString(&table_info->schema_) : "deleted")
                << std::endl;
      undo_link = undo_log.prev_version_;
      if (!undo_link.IsValid()) {
        break;
      }
      undo_log = txn_mgr->txn_map_.at(undo_link.prev_txn_)->GetUndoLog(undo_link.prev_log_idx_);
    }
  }
  std::cout << ">>>>>>>>>>>>>>>>>>>>>" << std::endl;
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
