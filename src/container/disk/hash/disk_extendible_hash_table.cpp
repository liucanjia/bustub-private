//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  this->index_name_ = name + "_idx";
  auto header_guard = this->bpm_->NewPageGuarded(&this->header_page_id_).UpgradeWrite();
  auto header_page = header_guard.template AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = this->Hash(key);
  // try get the header page
  ReadPageGuard header_guard = this->bpm_->FetchPageRead(this->header_page_id_);
  if (header_guard.IsNull()) {
    return false;
  }
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  // try get the directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();  // free header page to save the buffer_pool
  ReadPageGuard directory_guard = this->bpm_->FetchPageRead(directory_page_id);
  if (directory_guard.IsNull()) {
    return false;
  }
  auto dirctory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  // try get the bucket page
  auto bucket_idx = dirctory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dirctory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();  // free directory page to save the buffer_pool
  ReadPageGuard bucket_guard = this->bpm_->FetchPageRead(bucket_page_id);
  if (bucket_guard.IsNull()) {
    return false;
  }
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  // look for value
  V value;
  if (bucket_page->Lookup(key, value, this->cmp_)) {
    result->emplace_back(std::move(value));
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  auto hash = this->Hash(key);

  WritePageGuard header_guard = this->bpm_->FetchPageWrite(this->header_page_id_);
  if (header_guard.IsNull()) {
    return false;
  }
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  // try to get the directory page
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    // directory entry is not exist, create enrty and directly insert
    return this->InsertToNewDirectory(std::move(header_guard), header_page, directory_idx, hash, key, value);
  }

  header_guard.Drop();  // free header page to save the buffer_pool
  WritePageGuard directory_guard = this->bpm_->FetchPageWrite(directory_page_id);
  if (directory_guard.IsNull()) {
    return false;
  }
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  // try to get the bucket page
  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    // bucket entry is not exist, create enrty and directly insert
    return this->InsertToNewBucket(std::move(directory_guard), directory_page, bucket_idx, key, value);
  }

  WritePageGuard bucket_guard = this->bpm_->FetchPageWrite(bucket_page_id);
  if (bucket_guard.IsNull()) {
    return false;
  }
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // if bucket page is full
  while (bucket_page->IsFull()) {
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t global_depth = directory_page->GetGlobalDepth();
    uint32_t max_depth = directory_page->GetMaxDepth();
    // hash table is full, can not insert new entry
    if (local_depth == max_depth) {
      return false;
    }
    // get a new bucket page
    page_id_t split_bucket_page_id = INVALID_PAGE_ID;
    WritePageGuard split_bucket_guard = this->bpm_->NewPageGuarded(&split_bucket_page_id).UpgradeWrite();
    if (split_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    auto split_bucket_page = split_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    split_bucket_page->Init(this->bucket_max_size_);
    uint32_t split_idx = INVALID_PAGE_ID;

    // if bucket entry local_depth == global_depth, increment the global_depth
    if (local_depth == global_depth) {
      split_idx = directory_page->GetSplitImageIndex(bucket_idx, global_depth + 1);
      directory_page->IncrLocalDepth(bucket_idx);
      directory_page->IncrGlobalDepth();
      this->UpdateDirectoryMapping(directory_page, split_idx, split_bucket_page_id, local_depth + 1);
    } else if (local_depth < global_depth) {
      // if bucket entry local_depth < global_depth, increment the local_depth, remap the bucket_page_id
      uint32_t diff = 1 << local_depth;
      uint32_t tmp_idx = (directory_page->GetLocalDepthMask(bucket_idx) & bucket_idx);
      split_idx = tmp_idx + diff;
      diff <<= 1;
      // update local_depth
      for (; tmp_idx < directory_page->Size(); tmp_idx += diff) {
        this->UpdateDirectoryMapping(directory_page, tmp_idx, bucket_page_id, local_depth + 1);
      }
      // update bucket_page_id and local_depth
      for (tmp_idx = split_idx; tmp_idx < directory_page->Size(); tmp_idx += diff) {
        this->UpdateDirectoryMapping(directory_page, tmp_idx, split_bucket_page_id, local_depth + 1);
      }
    }
    // migrate entries
    this->MigrateEntries(bucket_page, split_bucket_page, split_idx, directory_page->GetLocalDepthMask(split_idx));

    // bucket entry may change, reget the bucket page
    bucket_idx = directory_page->HashToBucketIndex(hash);
    if (page_id_t tmp_page_id = directory_page->GetBucketPageId(bucket_idx); tmp_page_id == split_bucket_page_id) {
      bucket_page_id = split_bucket_page_id;
      bucket_guard = std::move(split_bucket_guard);
      bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    }
  }

  directory_guard.Drop();
  return bucket_page->Insert(key, value, this->cmp_);
  ;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(WritePageGuard &&header_guard,
                                                             ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id = INVALID_PAGE_ID;
  WritePageGuard directory_guard = this->bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(this->directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  header_guard.Drop();
  return this->InsertToNewBucket(std::move(directory_guard), directory_page, 0, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(WritePageGuard &&directory_guard,
                                                          ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  WritePageGuard bucket_guard = this->bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(this->bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  directory_guard.Drop();
  return bucket_page->Insert(key, value, this->cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  for (size_t i = 0; i < old_bucket->Size();) {
    if (auto hash = this->Hash(old_bucket->KeyAt(i));
        (hash & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      auto kv_pair = old_bucket->EntryAt(i);
      old_bucket->RemoveAt(i);
      new_bucket->Insert(kv_pair.first, kv_pair.second, this->cmp_);
    } else {
      i++;
    }
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  auto hash = this->Hash(key);
  // try to get the header page
  ReadPageGuard header_guard = this->bpm_->FetchPageRead(this->header_page_id_);
  if (header_guard.IsNull()) {
    return false;
  }
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  // directory entry is not exist
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();  // free header page to save the buffer_pool

  WritePageGuard directory_guard = this->bpm_->FetchPageWrite(directory_page_id);
  if (directory_guard.IsNull()) {
    return false;
  }
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  // bucket entry is not exist
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_guard = this->bpm_->FetchPageWrite(bucket_page_id);
  if (bucket_guard.IsNull()) {
    return false;
  }
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  if (bool result = bucket_page->Remove(key, this->cmp_); !result) {
    return false;
  }

  // merge the bucket when bucket_page or split_bucket_page is empty
  while (directory_page->GetLocalDepth(bucket_idx) > 0) {
    // get the split bucket entry local_depth
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t tmp_idx = bucket_idx & directory_page->GetLocalDepthMask(bucket_idx);
    uint32_t split_idx = directory_page->GetSplitImageIndex(tmp_idx, local_depth);
    uint32_t split_depth = directory_page->GetLocalDepth(split_idx);
    if (local_depth != split_depth) {
      break;
    }

    page_id_t split_bucket_page_id = directory_page->GetBucketPageId(split_idx);
    WritePageGuard split_bucket_page_guard = this->bpm_->FetchPageWrite(split_bucket_page_id);
    if (split_bucket_page_guard.IsNull()) {
      break;  // if bucket and split bucket don't have the same depth, don't merge
    }

    auto split_bucket_page = split_bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket_page->IsEmpty() && !split_bucket_page->IsEmpty()) {
      break;  // if bucket and split bucket are not empty, don't merge
    }

    page_id_t merge_page_id = INVALID_PAGE_ID;
    if (!bucket_page->IsEmpty()) {
      merge_page_id = bucket_page_id;
    } else {
      merge_page_id = split_bucket_page_id;
    }

    // merge bucket and split bucket
    uint32_t idx = bucket_idx = std::min(tmp_idx, split_idx);
    uint32_t diff = std::max(tmp_idx, split_idx) - idx;
    for (; idx < directory_page->Size(); idx += diff) {
      this->UpdateDirectoryMapping(directory_page, idx, merge_page_id, local_depth - 1);
    }

    // update bucket entry
    if (bucket_page_id != merge_page_id) {
      bucket_page_id = split_bucket_page_id;
      bucket_guard = std::move(split_bucket_page_guard);
      bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    }

    // try to shrink directory
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }
  }

  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
