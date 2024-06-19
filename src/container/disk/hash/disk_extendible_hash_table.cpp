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
  /*
  初始化header_page
  BasicPageGuard header_guard对应缓冲区中的一个页，它是pin的，且会在生命周期结束的时候自动unpin
  header_page是指向header_guard对象的一个指针，表示的是header_guard中的数据，并被翻译成ExtendibleHTableHeaderPage格式
  所以必须现有header_guard才能有header_page（得先将此页调度到缓存才行）
  */
  BasicPageGuard basic_header_guard = bpm_->NewPageGuarded(&header_page_id_);
  // 这里升级是为了加写锁，避免多线程发生错误
  auto header_guard = basic_header_guard.UpgradeWrite();
  // 这里需要guard.AsMut是因为Init函数需要修改数据
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth);

  /*
          // 初始化各个directory_page
          page_id_t directory_page_id;
          page_id_t bucket_page_id;
          for(uint32_t i=0; i<(static_cast<uint32_t>(1)<<header_max_depth); ++i) {
                  BasicPageGuard basic_directory_guard = bpm->NewPageGuarded(&directory_page_id);
                  auto directory_guard = basic_directory_guard.UpgradeWrite();
                  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
                  directory_page->Init(directory_max_depth);

                  BasicPageGuard basic_bucket_guard = bpm->NewPageGuarded(&bucket_page_id);
                  auto bucket_guard = basic_bucket_guard.UpgradeWrite();
                  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
                  bucket_page->Init(bucket_max_size);

                  directory_page->SetBucketPageId(0, bucket_page_id);

                  header_page->SetDirectoryPageId(i, directory_page_id);
          }
  */
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = Hash(key);

  // 获取header_page
  ReadPageGuard header_guard = bpm_->FetchPageRead(header_page_id_);
  // 这里用guard.As即可，因为不需要可变数据
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();

  // 通过header_page来获取hash值对应的directory_page_id是哪一个
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();

  // 获取directory_page
  ReadPageGuard directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();

  // 通过directory_page来获取hash值对应的bucket_page_id是哪一个
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  directory_guard.Drop();

  // 获取bucket_page
  ReadPageGuard bucket_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  // 通过bucket_page查找值
  V value;
  if (!bucket_page->Lookup(key, value, cmp_)) {
    return false;
  }
  result->push_back(value);

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  // 确保不插入重复的元素
  std::vector<V> temp_result;
  if(GetValue(key, &temp_result)) {
	return false;
  }

  uint32_t hash = Hash(key);

  // 获取可写的header_page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // 判断要将此数据插入哪个directory_page
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    // 调用插入新directory的函数
    if (!NewDirectory(header_page, directory_idx, hash)) {
      return false;
    }
    directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  }
  header_guard.Drop();

  // 获取可写的directory_page
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 通过directory_page来获取hash值对应的bucket_page_id是哪一个
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    // 调用插入新bucket的函数
    if (!NewBucket(directory_page, bucket_idx)) {
      return false;
    }
    bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  }
  directory_guard.Drop();

  // 获取可写的bucket_page
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 正式插入数据
  if(bucket_page->size_ == bucket_page->max_size_) {
    SpliteBucket();
  }
  if(bucket_page->Insert(key, value, cmp_)) {
	return true;
  }


  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::NewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                     uint32_t hash) -> bool {
  // 新建一个directory_page
  page_id_t directory_page_id;
  BasicPageGuard basic_directory_guard = bpm_->NewPageGuarded(&directory_page_id);
  auto directory_guard = basic_directory_guard.UpgradeWrite();
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);

  header->SetDirectoryPageId(directory_idx, directory_page_id);
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::NewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx)
    -> bool {
  // 新建一个bucket_page
  page_id_t bucket_page_id;
  BasicPageGuard basic_bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard = basic_bucket_guard.UpgradeWrite();
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);

  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  return false;
}

/*
在 C++ 中，模板类需要显式实例化（explicit instantiation）才能生成特定类型的实例。
通过模板实例化，可以预先生成并编译模板类的特定类型实例，以减少编译时间并提高编译器的性能。
这样可以避免每次使用模板类时都需要重新实例化。
*/
template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
