//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto schema = this->table_info_->schema_;
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();

  std::vector<std::pair<Tuple, RID>> delete_tuples;
  while (this->child_executor_->Next(tuple, rid)) {
    delete_tuples.emplace_back(std::make_pair(std::move(*tuple), *rid));
  }

  for (auto &&[delete_tuple, delete_rid] : delete_tuples) {
    TupleMeta old_meta;
    VersionUndoLink version_link;
    PreCheck("Delete", delete_rid, old_meta, this->table_info_, version_link, txn, txn_mgr);

    // self-modification
    if (old_meta.ts_ == txn->GetTransactionId()) {
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
            vals.emplace_back(delete_tuple.GetValue(&schema, idx));
          }
        }

        undo_log.tuple_ = Tuple{vals, &schema};
        txn->ModifyUndoLog(log_idx, undo_log);
      }
    } else {
      // generate the undo log, and link them together
      UndoLog undo_log{false, std::vector<bool>(schema.GetColumnCount(), true), delete_tuple, old_meta.ts_};
      if (version_link.prev_.IsValid()) {
        undo_log.prev_version_ = version_link.prev_;
      }

      auto undo_link = txn->AppendUndoLog(undo_log);
      version_link.prev_ = undo_link;
      txn->AppendWriteSet(this->table_info_->oid_, delete_rid);
    }

    // update the tuple meta
    this->table_info_->table_->UpdateTupleMeta({txn->GetTransactionId(), true}, delete_rid);

    version_link.in_progress_ = false;
    txn_mgr->UpdateVersionLink(delete_rid, std::make_optional(version_link), nullptr);
  }

  if (!this->delete_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(delete_tuples.size())}, &this->GetOutputSchema()};
    this->delete_finished_ = true;
    return true;
  }

  return false;
}

// auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   Tuple delete_tuple;
//   RID delete_rid;
//   int deleted_count = 0;

//   while (this->child_executor_->Next(&delete_tuple, &delete_rid)) {
//     // delete tuple
//     this->table_info_->table_->UpdateTupleMeta({0, true}, delete_rid);
//     ++deleted_count;

//     // update the indexs
//     for (auto index_info : this->table_indexs_) {
//       auto key_schema = *index_info->index_->GetKeySchema();
//       auto key_attrs = index_info->index_->GetKeyAttrs();
//       index_info->index_->DeleteEntry(delete_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
//                                       delete_rid, this->exec_ctx_->GetTransaction());
//     }
//   }

//   if (!this->delete_finished_) {
//     *tuple = Tuple{{ValueFactory::GetIntegerValue(deleted_count)}, &this->GetOutputSchema()};
//     this->delete_finished_ = true;
//     return true;
//   }

//   return false;
// }

}  // namespace bustub
