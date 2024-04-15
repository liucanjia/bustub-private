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

auto IsExistIndex(Tuple &tuple, Schema &schema, const std::vector<IndexInfo *> &table_indexs, Transaction *txn,
                  std::vector<RID> &result) -> bool {
  // check if the tuple already exists in the index
  for (auto index_info : table_indexs) {
    auto key_schema = *index_info->index_->GetKeySchema();
    auto key_attrs = index_info->index_->GetKeyAttrs();
    index_info->index_->ScanKey(tuple.KeyFromTuple(schema, key_schema, key_attrs), &result, txn);
    // all index will return same result
    if (!result.empty()) {
      return true;
    }
  }
  return false;
}

void InsertTuple(Tuple &tuple, Schema &schema, const TableInfo *table_info,
                 const std::vector<IndexInfo *> &table_indexs, Transaction *txn, TransactionManager *txn_mgr) {
  // check if the tuple already exists in the index
  std::vector<RID> result;

  if (IsExistIndex(tuple, schema, table_indexs, txn, result)) {
    BUSTUB_ASSERT(result.size() <= 1, "Only primary index, so index must be exist one or none.\n");

    auto old_rid = result[0];
    auto [old_meta, old_tuple] = table_info->table_->GetTuple(old_rid);

    // if index already exists and old tuple is delete, update the tuple inplace
    if ((old_meta.ts_ < TXN_START_ID || old_meta.ts_ == txn->GetTransactionId()) && old_meta.is_deleted_) {
      if (old_meta.ts_ < TXN_START_ID) {
        VersionUndoLink version_link;
        PreCheck("Insert", old_rid, old_meta, table_info, version_link, txn, txn_mgr);

        // generate the undo log, and link them together
        UndoLog undo_log{true, std::vector<bool>(schema.GetColumnCount(), true), old_tuple, old_meta.ts_};
        if (auto undo_link = version_link.prev_; undo_link.IsValid()) {
          undo_log.prev_version_ = undo_link;
        }

        auto undo_link = txn->AppendUndoLog(undo_log);
        version_link.prev_ = undo_link;
        txn->AppendWriteSet(table_info->oid_, old_rid);

        table_info->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, tuple, old_rid);
        version_link.in_progress_ = false;
        txn_mgr->UpdateVersionLink(old_rid, std::make_optional(version_link), nullptr);
      } else {
        // update tuple inplace
        table_info->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, tuple, old_rid);
      }
      return;
    }
    // tuple already exist, abort the transaction
    txn->SetTainted();
    throw ExecutionException("Insert tuple has already exist.\n");
  }

  InsertNewTuple(tuple, table_info, table_indexs, txn);
}

void InsertNewTuple(Tuple &tuple, const TableInfo *table_info, const std::vector<IndexInfo *> &table_indexs,
                    Transaction *txn) {
  // insert new tuple
  if (auto new_rid = table_info->table_->InsertTuple({txn->GetTransactionId(), false}, tuple);
      new_rid != std::nullopt) {
    // update the transaction write set
    txn->AppendWriteSet(table_info->oid_, new_rid.value());
    // update the indexs
    for (auto index_info : table_indexs) {
      auto key_schema = *index_info->index_->GetKeySchema();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      // create index
      if (!index_info->index_->InsertEntry(tuple.KeyFromTuple(table_info->schema_, key_schema, key_attrs),
                                           new_rid.value(), txn)) {
        // if index already exist, throw exception
        txn->SetTainted();
        throw ExecutionException("Insert tuple has already exist.\n");
      }
    }
  }
}

void DeleteTuple(const Tuple &tuple, RID rid, Schema &schema, const TableInfo *table_info, Transaction *txn,
                 TransactionManager *txn_mgr) {
  TupleMeta meta;
  VersionUndoLink version_link;
  PreCheck("Delete", rid, meta, table_info, version_link, txn, txn_mgr);

  // self-modification
  if (meta.ts_ == txn->GetTransactionId()) {
    // if has undo_log, update the undo_log
    if (version_link.prev_.IsValid()) {
      auto [prev_txn, log_idx] = version_link.prev_;
      BUSTUB_ASSERT(prev_txn == txn->GetTransactionId(), "if self-modification, prev_version must in itself.\n");

      auto undo_log = txn->GetUndoLog(log_idx);

      std::vector<Column> cols;
      for (size_t idx = 0; idx < schema.GetColumnCount(); ++idx) {
        if (undo_log.modified_fields_[idx]) {
          cols.emplace_back(schema.GetColumn(idx));
        }
      }
      Schema old_modified_schema = Schema{cols};

      std::vector<Value> vals;
      for (size_t idx = 0, modified_idx = 0; idx < schema.GetColumnCount(); ++idx) {
        if (undo_log.modified_fields_[idx]) {
          vals.emplace_back(undo_log.tuple_.GetValue(&old_modified_schema, modified_idx++));
        } else {
          undo_log.modified_fields_[idx] = true;
          vals.emplace_back(tuple.GetValue(&schema, idx));
        }
      }

      undo_log.tuple_ = Tuple{vals, &schema};
      txn->ModifyUndoLog(log_idx, undo_log);
    }
  } else {
    // generate the undo log, and link them together
    UndoLog undo_log{false, std::vector<bool>(schema.GetColumnCount(), true), tuple, meta.ts_};
    if (version_link.prev_.IsValid()) {
      undo_log.prev_version_ = version_link.prev_;
    }

    auto undo_link = txn->AppendUndoLog(undo_log);
    version_link.prev_ = undo_link;
    txn->AppendWriteSet(table_info->oid_, rid);
  }

  // update the tuple meta
  table_info->table_->UpdateTupleMeta({txn->GetTransactionId(), true}, rid);

  version_link.in_progress_ = false;
  txn_mgr->UpdateVersionLink(rid, std::make_optional(version_link), nullptr);
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
