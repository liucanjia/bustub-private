#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void GetTupleByTimetamp(Tuple &tuple, TupleMeta &tuple_meta, RID &rid, Schema &schema, TransactionManager *txn_mgr,
                        Transaction *txn);
// void GetTupleByTimetamp(Tuple &tuple, TupleMeta &tuple_meta, RID &rid, Schema &schema, TransactionManager *txn_mgr,
//                         Transaction *txn, VersionUndoLink &version_link);

void PreCheck(std::string_view type, RID &rid, TupleMeta &meta, const TableInfo *table_info,
              VersionUndoLink &version_link, Transaction *txn, TransactionManager *txn_mgr);

auto IsExistIndex(Tuple &tuple, Schema &schema, const std::vector<IndexInfo *> &table_indexs, Transaction *txn,
                  std::vector<RID> &result) -> bool;

void InsertTuple(Tuple &tuple, Schema &schema, const TableInfo *table_info,
                 const std::vector<IndexInfo *> &table_indexs, Transaction *txn, TransactionManager *txn_mgr);

void InsertNewTuple(Tuple &tuple, const TableInfo *table_info, const std::vector<IndexInfo *> &table_indexs,
                    Transaction *txn);

void DeleteTuple(const Tuple &tuple, RID rid, Schema &schema, const TableInfo *table_info, Transaction *txn,
                 TransactionManager *txn_mgr);

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
