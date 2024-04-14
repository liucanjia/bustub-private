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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  // init the child_executor first
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple insert_tuple;
  RID old_rid;
  int insertd_count = 0;
  auto schema = this->table_info_->schema_;
  auto timestamp = this->exec_ctx_->GetTransaction()->GetTransactionId();
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();

  while (this->child_executor_->Next(&insert_tuple, &old_rid)) {
    // check if the tuple already exists in the index
    ++insertd_count;
    std::vector<RID> result;
    for (auto index_info : this->table_indexs_) {
      auto key_schema = *index_info->index_->GetKeySchema();
      auto key_attrs = index_info->index_->GetKeyAttrs();
      index_info->index_->ScanKey(insert_tuple.KeyFromTuple(schema, key_schema, key_attrs), &result,
                                  this->exec_ctx_->GetTransaction());
      // all index will return same result
      if (!result.empty()) {
        break;
      }
    }
    BUSTUB_ASSERT(result.size() <= 1, "Only primary index, so index must be exist one or none.\n");

    if (!result.empty()) {
      auto old_rid = result[0];
      auto [old_meta, old_tuple] = this->table_info_->table_->GetTuple(old_rid);

      // if index already exists and old tuple is delete, update the tuple inplace
      if ((old_meta.ts_ < TXN_START_ID || old_meta.ts_ == txn->GetTransactionId()) && old_meta.is_deleted_) {
        if (old_meta.ts_ < TXN_START_ID) {
          VersionUndoLink version_link;
          PreCheck("Insert", old_rid, old_meta, this->table_info_, version_link, txn, txn_mgr);

          // generate the undo log, and link them together
          UndoLog undo_log{true, std::vector<bool>(schema.GetColumnCount(), true), old_tuple, old_meta.ts_};
          if (auto undo_link = version_link.prev_; undo_link.IsValid()) {
            undo_log.prev_version_ = undo_link;
          }

          auto undo_link = txn->AppendUndoLog(undo_log);
          version_link.prev_ = undo_link;
          txn->AppendWriteSet(this->table_info_->oid_, old_rid);

          this->table_info_->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, insert_tuple, old_rid);
          version_link.in_progress_ = false;
          txn_mgr->UpdateVersionLink(old_rid, std::make_optional(version_link), nullptr);
        } else {
          // update tuple inplace
          this->table_info_->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, insert_tuple, old_rid);
        }
        continue;
      }
      // tuple already exist, abort the transaction
      txn->SetTainted();
      throw ExecutionException("Insert tuple has already exist.\n");
    }

    // insert new tuple
    if (auto new_rid = this->table_info_->table_->InsertTuple({timestamp, false}, insert_tuple);
        new_rid != std::nullopt) {
      // update the transaction write set
      txn->AppendWriteSet(this->table_info_->oid_, new_rid.value());
      // update the indexs
      for (auto index_info : this->table_indexs_) {
        auto key_schema = *index_info->index_->GetKeySchema();
        auto key_attrs = index_info->index_->GetKeyAttrs();
        // create index
        if (!index_info->index_->InsertEntry(
                insert_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs), new_rid.value(),
                this->exec_ctx_->GetTransaction())) {
          // if index already exist, throw exception
          txn->SetTainted();
          throw ExecutionException("Insert tuple has already exist.\n");
        }
      }
    }
  }

  if (!this->insert_finished_) {
    *tuple = Tuple{{ValueFactory::GetIntegerValue(insertd_count)}, &this->GetOutputSchema()};
    this->insert_finished_ = true;
    return true;
  }

  return false;
}

}  // namespace bustub
