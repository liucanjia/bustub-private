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

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  this->child_executor_->Init();

  auto catalog = this->exec_ctx_->GetCatalog();
  this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
  this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto target_exprs = this->plan_->target_expressions_;
  auto schema = this->table_info_->schema_;
  auto txn = this->exec_ctx_->GetTransaction();
  auto txn_mgr = this->exec_ctx_->GetTransactionManager();

  std::vector<std::pair<Tuple, RID>> old_tuples;
  while (this->child_executor_->Next(tuple, rid)) {
    old_tuples.emplace_back(std::make_pair(std::move(*tuple), *rid));
  }

  for (auto &&[old_tuple, old_rid] : old_tuples) {
    std::vector<Value> tuple_vals;
    tuple_vals.reserve(target_exprs.size());
    for (const auto &expr : target_exprs) {
      tuple_vals.emplace_back(expr->Evaluate(&old_tuple, schema));
    }

    auto new_tuple = Tuple{tuple_vals, &schema};

    TupleMeta old_meta;
    VersionUndoLink version_link;
    PreCheck("Update", old_rid, old_meta, this->table_info_, version_link, txn, txn_mgr);

    if (old_meta.ts_ == txn->GetTransactionId()) {
      // self-modification
      if (!IsTupleContentEqual(old_tuple, new_tuple)) {
        // if has undo_log, update the undo_log
        if (auto undo_link = version_link.prev_; undo_link.IsValid()) {
          BUSTUB_ASSERT(undo_link.prev_txn_ == txn->GetTransactionId(),
                        "if self-modification, prev_version must in itself.\n");

          auto undo_log = txn->GetUndoLog(undo_link.prev_log_idx_);

          std::vector<Column> cols;
          for (size_t idx = 0; idx < schema.GetColumnCount(); ++idx) {
            if (undo_log.modified_fields_[idx]) {
              cols.emplace_back(schema.GetColumn(idx));
            }
          }
          Schema old_modified_schema = Schema{cols};
          cols.clear();

          std::vector<Value> vals;
          for (size_t idx = 0, modified_idx = 0; idx < schema.GetColumnCount(); ++idx) {
            if (undo_log.modified_fields_[idx]) {
              vals.emplace_back(undo_log.tuple_.GetValue(&old_modified_schema, modified_idx++));
              cols.emplace_back(schema.GetColumn(idx));
            } else {
              auto old_val = old_tuple.GetValue(&schema, idx);
              auto new_val = new_tuple.GetValue(&schema, idx);
              if (!old_val.CompareExactlyEquals(new_val)) {
                undo_log.modified_fields_[idx] = true;
                vals.emplace_back(old_val);
                cols.emplace_back(schema.GetColumn(idx));
              }
            }
          }

          Schema new_modified_schema = Schema{cols};
          undo_log.tuple_ = Tuple{vals, &new_modified_schema};
          txn->ModifyUndoLog(undo_link.prev_log_idx_, undo_log);
        }

        // update the tuple
        this->table_info_->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, new_tuple, old_rid);
      }
    } else {
      if (!IsTupleContentEqual(old_tuple, new_tuple)) {
        // generate the undo log, and link them together
        std::vector<bool> modified_filed(schema.GetColumnCount(), false);
        std::vector<Value> vals;
        std::vector<Column> modified_cols;

        for (size_t idx = 0; idx < schema.GetColumnCount(); ++idx) {
          auto old_val = old_tuple.GetValue(&schema, idx);
          auto new_val = new_tuple.GetValue(&schema, idx);
          if (!old_val.CompareExactlyEquals(new_val)) {
            modified_filed[idx] = true;
            vals.emplace_back(std::move(old_val));
            modified_cols.emplace_back(schema.GetColumn(idx));
          }
        }

        Schema modified_schema{modified_cols};
        UndoLog undo_log{false, std::move(modified_filed), Tuple{vals, &modified_schema}, old_meta.ts_};
        if (version_link.prev_.IsValid()) {
          undo_log.prev_version_ = version_link.prev_;
        }

        auto undo_link = txn->AppendUndoLog(undo_log);
        version_link.prev_ = undo_link;
        txn->AppendWriteSet(this->table_info_->oid_, old_rid);
        this->table_info_->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, new_tuple, old_rid);
      }
    }

    version_link.in_progress_ = false;
    txn_mgr->UpdateVersionLink(old_rid, std::make_optional(version_link), nullptr);
  }

  if (!this->update_finished_) {
    if (old_tuples.empty()) {
      // 存在无法从底下算子拿到tuple的情况, 这时要抛出错误(暂时不懂为什么地下Scan算子返回为空)
      txn->SetTainted();
      throw ExecutionException("");
    }
    *tuple = Tuple{{ValueFactory::GetIntegerValue(old_tuples.size())}, &this->plan_->OutputSchema()};
    this->update_finished_ = true;
    return true;
  }

  return false;
}

// void UpdateExecutor::Init() {
//   this->child_executor_->Init();

//   auto catalog = this->exec_ctx_->GetCatalog();
//   this->table_info_ = catalog->GetTable(this->plan_->GetTableOid());
//   this->table_indexs_ = catalog->GetTableIndexes(this->table_info_->name_);
// }

// auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//   Tuple update_tuple;
//   RID old_rid;
//   int updated_count = 0;
//   auto target_exprs = this->plan_->target_expressions_;
//   auto schema = this->table_info_->schema_;

//   while (this->child_executor_->Next(&update_tuple, &old_rid)) {
//     // firstly, delete the affected tuple and index
//     this->table_info_->table_->UpdateTupleMeta({0, true}, old_rid);
//     for (auto index_info : this->table_indexs_) {
//       auto key_schema = *index_info->index_->GetKeySchema();
//       auto key_attrs = index_info->index_->GetKeyAttrs();
//       index_info->index_->DeleteEntry(update_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
//                                       old_rid, this->exec_ctx_->GetTransaction());
//     }

//     // update the tuple
//     std::vector<Value> tuple_values;
//     tuple_values.reserve(target_exprs.size());
//     for (const auto &expr : target_exprs) {
//       tuple_values.emplace_back(expr->Evaluate(&update_tuple, schema));
//     }
//     update_tuple = Tuple{tuple_values, &schema};
//     // insert a new tuple
//     if (auto new_rid = this->table_info_->table_->InsertTuple({0, false}, update_tuple); new_rid != std::nullopt) {
//       ++updated_count;
//       // update the indexs
//       for (auto index_info : this->table_indexs_) {
//         auto key_schema = *index_info->index_->GetKeySchema();
//         auto key_attrs = index_info->index_->GetKeyAttrs();
//         index_info->index_->InsertEntry(update_tuple.KeyFromTuple(this->table_info_->schema_, key_schema, key_attrs),
//                                         new_rid.value(), this->exec_ctx_->GetTransaction());
//       }
//     }
//   }

//   if (!this->update_finished_) {
//     *tuple = Tuple{{ValueFactory::GetIntegerValue(updated_count)}, &this->plan_->OutputSchema()};
//     this->update_finished_ = true;
//     return true;
//   }

//   return false;
// }

}  // namespace bustub
