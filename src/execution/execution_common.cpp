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
  bool exist = !base_meta.is_deleted_;
  size_t col_cnt = schema->GetColumnCount();
  std::vector<Value> vals(col_cnt);

  if (!base_meta.is_deleted_) {
    for (size_t idx = 0; idx < col_cnt; ++idx) {
      vals[idx] = base_tuple.GetValue(schema, idx);
    }
  }

  for (auto &&undo_log : undo_logs) {
    exist = !undo_log.is_deleted_;
    if (!undo_log.is_deleted_) {
      std::vector<Column> cols;
      for (size_t idx = 0; idx < col_cnt; ++idx) {
        if (undo_log.modified_fields_[idx]) {
          cols.emplace_back(schema->GetColumn(idx));
        }
      }

      Schema modified_schema{cols};
      for (size_t idx = 0, modified_idx = 0; idx < col_cnt; ++idx) {
        if (undo_log.modified_fields_[idx]) {
          vals[idx] = undo_log.tuple_.GetValue(&modified_schema, modified_idx++);
        }
      }
    }
  }

  if (!exist) {
    return std::nullopt;
  }

  return std::make_optional(Tuple{vals, schema});
}

void GetTupleByTimetamp(Tuple &tuple, TupleMeta &tuple_meta, RID &rid, Schema &schema, TransactionManager *txn_mgr,
                        Transaction *txn) {
  // The tuple modified by another uncommitted transaction or The tuple newer than the transaction read timestamp
  if ((tuple_meta.ts_ >= TXN_START_ID && tuple_meta.ts_ != txn->GetTransactionId()) ||
      (tuple_meta.ts_ < TXN_START_ID && tuple_meta.ts_ > txn->GetReadTs())) {
    std::vector<UndoLog> undo_logs;

    bool final_log = false;
    if (auto opt_version_link = txn_mgr->GetVersionLink(rid); opt_version_link.has_value()) {
      auto version_link = opt_version_link.value();
      for (auto undo_link = version_link.prev_; undo_link.IsValid() && !final_log;) {
        if (auto opt_undo_log = txn_mgr->GetUndoLogOptional(undo_link); opt_undo_log.has_value()) {
          auto undo_log = opt_undo_log.value();
          undo_logs.emplace_back(undo_log);
          undo_link = undo_log.prev_version_;
          if (undo_log.ts_ <= txn->GetReadTs()) {
            final_log = true;
          }
        }
      }
    }

    if (final_log) {
      auto opt_tuple = ReconstructTuple(&schema, tuple, tuple_meta, undo_logs);
      if (opt_tuple.has_value()) {
        tuple = opt_tuple.value();
        tuple_meta.is_deleted_ = undo_logs.back().is_deleted_;
        tuple_meta.ts_ = undo_logs.back().ts_;
        return;
      }
    }
    // The tuple was deleted or the Transaction read timestamp older than all tuple
    tuple_meta.is_deleted_ = true;
    tuple_meta.ts_ = 0;
  }

  // The tuple in the table heap is the most recent data or The tuple in the table heap contains modification by the
  // current transaction
}

void PreCheck(std::string_view type, RID &rid, TupleMeta &meta, const TableInfo *table_info,
              VersionUndoLink &version_link, Transaction *txn, TransactionManager *txn_mgr) {
  std::stringstream ss;
  ss << type << " W-W conflict.\n";

  while (true) {
    meta = table_info->table_->GetTupleMeta(rid);

    // check W-W conflict
    if ((meta.ts_ >= TXN_START_ID && meta.ts_ != txn->GetTransactionId()) ||
        (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs())) {
      // W-W conflict
      txn->SetTainted();
      throw ExecutionException(ss.str());
    }

    // check in_progress
    while (true) {
      auto opt_version_link = txn_mgr->GetVersionLink(rid);
      if (opt_version_link.has_value()) {
        version_link = opt_version_link.value();
        if (version_link.in_progress_) {
          // if tuple is modifying by other transaction, spin to wait
          continue;
        }
      }
      // if version_link is null or tuple isn't modifying, go to the next step
      break;
    }

    // try to update version link, if update failed, try the setps again
    version_link.in_progress_ = true;
    if (!txn_mgr->UpdateVersionLink(rid, std::make_optional(version_link),
                                    [](std::optional<VersionUndoLink> opt_version_link_) -> bool {
                                      if (!opt_version_link_.has_value()) {
                                        return true;
                                      }

                                      auto version_link = opt_version_link_.value();
                                      return !version_link.in_progress_;
                                    })) {
      continue;
    }

    // check W-W conflict again
    meta = table_info->table_->GetTupleMeta(rid);
    if ((meta.ts_ >= TXN_START_ID && meta.ts_ != txn->GetTransactionId()) ||
        (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs())) {
      // set in_progress = false
      version_link.in_progress_ = false;
      txn_mgr->UpdateVersionLink(rid, std::make_optional(version_link), nullptr);
      // W-W conflict
      txn->SetTainted();
      throw ExecutionException(ss.str());
    }

    // get the versionlink success
    break;
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  auto schema = table_info->schema_;
  for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
    auto [tuple_meta, tuple] = iter.GetTuple();
    auto rid = iter.GetRID();

    fmt::print("RID={}/{} ", rid.GetPageId(), rid.GetSlotNum());
    if (tuple_meta.ts_ >= TXN_START_ID) {
      fmt::print("ts=txn{} ", tuple_meta.ts_ ^ TXN_START_ID);
    } else {
      fmt::print("ts={} ", tuple_meta.ts_);
    }

    if (tuple_meta.is_deleted_) {
      fmt::print("<del marker> tuple(<NULL>");
      for (size_t i = 1; i < schema.GetColumnCount(); i++) {
        fmt::print(", <NULL>");
      }
    } else {
      if (auto val = tuple.GetValue(&schema, 0); val.IsNull()) {
        fmt::print("tuple(<NULL>");
      } else {
        fmt::print("tuple({}", tuple.GetValue(&schema, 0));
      }

      for (size_t i = 1; i < schema.GetColumnCount(); i++) {
        if (auto val = tuple.GetValue(&schema, i); val.IsNull()) {
          fmt::print(", <NULL>");
        } else {
          fmt::print(", {}", tuple.GetValue(&schema, i));
        }
      }
    }
    fmt::println(")");

    if (auto opt_undo_link = txn_mgr->GetUndoLink(rid); opt_undo_link.has_value()) {
      for (auto undo_link = opt_undo_link.value(); undo_link.IsValid();) {
        auto txn_id = undo_link.prev_txn_;
        if (auto opt_undo_log = txn_mgr->GetUndoLogOptional(undo_link); opt_undo_log.has_value()) {
          auto undo_log = opt_undo_log.value();

          std::vector<Column> cols;
          for (size_t i = 0; i < schema.GetColumnCount(); ++i) {
            if (undo_log.modified_fields_[i]) {
              cols.emplace_back(schema.GetColumn(i));
            }
          }

          Schema modified_schema{cols};

          fmt::print("  txn{}@{} ", txn_id ^ TXN_START_ID, undo_log.ts_);
          if (undo_log.is_deleted_) {
            fmt::println("<del> ts={}", undo_log.ts_);
          } else {
            fmt::print("(");

            size_t modified_idx = 0;
            if (undo_log.modified_fields_[0]) {
              if (auto val = undo_log.tuple_.GetValue(&modified_schema, modified_idx++); val.IsNull()) {
                fmt::print("<NULL>");
              } else {
                fmt::print("{}", val);
              }
            } else {
              fmt::print("_");
            }

            for (size_t i = 1; i < undo_log.modified_fields_.size(); i++) {
              if (undo_log.modified_fields_[i]) {
                if (auto val = undo_log.tuple_.GetValue(&modified_schema, modified_idx); val.IsNull()) {
                  fmt::print(", <NULL>");
                } else {
                  fmt::print(", {}", val);
                }
                ++modified_idx;
              } else {
                fmt::print(", _");
              }
            }
            fmt::println(") ts={}", undo_log.ts_);
          }

          undo_link = undo_log.prev_version_;
        } else {
          break;
        }
      }
    }
  }

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@1 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@2 <del> ts=2
  //   txn3@1 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@3 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@2 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
