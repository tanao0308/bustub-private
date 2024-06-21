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
#include <mutex>
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
  LOG_DEBUG("header_max_depth=%d, directory_max_depth=%d, bucket_max_size=%d", header_max_depth, directory_max_depth,
            bucket_max_size);
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
  assert(bucket_page_id != INVALID_PAGE_ID);
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
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  LOG_DEBUG("here");
  std::cout << "key=" << key << ", value=" << value << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  // 确保不插入重复的元素
  std::vector<V> temp_result;
  if (GetValue(key, &temp_result)) {
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
    NewDirectory(header_page, directory_idx, hash);
    directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  }
  header_guard.Drop();

  // 获取可写的directory_page
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 正式插入数据
  while (true) {
    // 通过directory_page来获取hash值对应的bucket_page_id是哪一个
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // 如果目标桶能插入成功了，那就返回true
    if (bucket_page->Insert(key, value, cmp_)) {
      return true;
    }

    // 如果插入失败，表示需要分裂，返回分裂后待插入的桶
    // 如果分裂成功，那继续循环，bucket_page已经更新了
    // 如果分裂失败，那就肯定不能插入了，将之前分裂出的空桶进行合并
    if (!SpliteBucket(directory_page, bucket_page, bucket_idx)) {
      MergeBucket(directory_page, bucket_idx);
      return false;
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::NewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                     uint32_t hash) {
  // LOG_DEBUG("here");
  // 新建一个directory_page
  page_id_t directory_page_id;
  BasicPageGuard basic_directory_guard = bpm_->NewPageGuarded(&directory_page_id);
  auto directory_guard = basic_directory_guard.UpgradeWrite();
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);

  header->SetDirectoryPageId(directory_idx, directory_page_id);

  // 为新的directory建一个bucket
  page_id_t bucket_page_id;
  BasicPageGuard basic_bucket_guard = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard = basic_bucket_guard.UpgradeWrite();
  auto bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket->Init(bucket_max_size_);

  directory->SetLocalDepth(0, 0);
  directory->SetBucketPageId(0, bucket_page_id);
}

// 这个函数是为了将bucket分裂一次，输出是否分裂成功并将bucket更新为hash指向的bucket
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SpliteBucket(ExtendibleHTableDirectoryPage *directory,
                                                     ExtendibleHTableBucketPage<K, V, KC> *bucket, uint32_t bucket_idx)
    -> bool {
  // LOG_DEBUG("here");
  // 判断是否能继续分裂
  uint32_t global_depth = directory->GetGlobalDepth();
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  if (local_depth == global_depth) {
    if (global_depth == directory_max_depth_) {
      return false;
    }
    directory->IncrGlobalDepth();
    global_depth++;
  }  // 只需调整到global_depth > local_depth即可

  // 创建旧bucket0
  page_id_t bucket0_page_id = directory->GetBucketPageId(bucket_idx);
  ExtendibleHTableBucketPage<K, V, KC> *bucket0 = bucket;
  // 创建新bucket1
  page_id_t bucket1_page_id;
  BasicPageGuard basic_bucket1_guard = bpm_->NewPageGuarded(&bucket1_page_id);
  auto bucket1_guard = basic_bucket1_guard.UpgradeWrite();
  auto bucket1 = bucket1_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket1->Init(bucket_max_size_);

  // 更新directory的local_depths和bucket_page_id
  uint32_t local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
  for (uint32_t i = 0; i < (static_cast<uint32_t>(1) << (global_depth - local_depth)); ++i) {
    uint32_t temp_bucket_idx = (bucket_idx & local_depth_mask) + (i << local_depth);
    // 更新local_depths
    directory->IncrLocalDepth(temp_bucket_idx);
    // 更新bucket_page_id
    if ((i & 1) == 0) {
      directory->SetBucketPageId(temp_bucket_idx, bucket0_page_id);
    } else {
      directory->SetBucketPageId(temp_bucket_idx, bucket1_page_id);
    }
  }

  // 从bucket0里删去键值对移动到bucket1里
  for (uint32_t i = 0; i < bucket0->Size(); ++i) {
    K temp_key = bucket0->KeyAt(i);
    uint32_t temp_hash = Hash(temp_key);
    // 如果在这一位上是1，则将这个键值对加入bucket1
    if ((temp_hash & (static_cast<uint32_t>(1) << local_depth)) != 0) {
      V temp_value = bucket0->ValueAt(i);
      std::cout << temp_key << std::endl;
      bucket1->Insert(temp_key, temp_value, cmp_);
    }
  }
  // 清空bucket0中的不合法键值对
  for (uint32_t i = 0; i < bucket0->Size(); ++i) {
    while (i < bucket0->Size()) {
      // 当这一位是1时，不断删除
      K temp_key = bucket0->KeyAt(i);
      uint32_t temp_hash = Hash(temp_key);
      if ((temp_hash & (static_cast<uint32_t>(1) << local_depth)) != 0) {
        bucket0->RemoveAt(i);
      } else {
        break;
      }
    }
  }

  // 更新local_depth
  local_depth++;

  // 将bucket记为目标值返回
  if ((bucket_idx & (static_cast<uint32_t>(1) << local_depth)) == 0) {
    bucket = bucket0;
  } else {
    bucket = bucket1;
  }

  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MergeBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx) {
  // LOG_DEBUG("here");
  // 如果是directory的唯一bucket，那无需merge
  std::cout << "bucket_idx=" << bucket_idx << " " << directory->GetGlobalDepth() << std::endl;
  if (directory->GetGlobalDepth() == 0) {
    return;
  }
  assert(directory->GetLocalDepth(bucket_idx) != 0);

  // 获取bucket的信息
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  // 获取镜像bucket的信息
  uint32_t image_bucket_idx = directory->GetSplitImageIndex(bucket_idx);
  page_id_t image_bucket_page_id = directory->GetBucketPageId(image_bucket_idx);
  WritePageGuard image_bucket_guard = bpm_->FetchPageWrite(image_bucket_page_id);
  auto image_bucket = image_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 如果不能合并（镜像页不为空或镜像页深度不一致）就立刻结束
  if (image_bucket->Size() != 0 || directory->GetLocalDepth(bucket_idx) != directory->GetLocalDepth(image_bucket_idx)) {
    return;
  }

  // 否则合并
  // 更新directory的local_depths和bucket_page_id
  uint32_t global_depth = directory->GetGlobalDepth();
  directory->DecrLocalDepth(bucket_idx);
  uint32_t local_depth = directory->GetLocalDepth(bucket_idx);
  uint32_t local_depth_mask = directory->GetLocalDepthMask(bucket_idx);
  for (uint32_t i = 0; i < (static_cast<uint32_t>(1) << (global_depth - local_depth)); ++i) {
    uint32_t temp_bucket_idx = (bucket_idx & local_depth_mask) + (i << local_depth);
    if (temp_bucket_idx == bucket_idx) {
      continue;
    }
    // 更新local_depths
    directory->DecrLocalDepth(temp_bucket_idx);
    // 更新bucket_page_id
    directory->SetBucketPageId(temp_bucket_idx, bucket_page_id);
  }
  // 丢弃guard并删除页
  image_bucket_guard.Drop();
  bpm_->DeletePage(image_bucket_page_id);

  // 更新global_depth
  while (directory->CanShrink()) {
    directory->DecrGlobalDepth();
    bucket_idx = directory->HashToBucketIndex(bucket_idx);
  }

  MergeBucket(directory, bucket_idx);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  LOG_DEBUG("here");
  std::cout << "key=" << key << std::endl;
  std::lock_guard<std::mutex> lock(latch_);

  // 若不存在则不能删除
  std::vector<V> result;
  if (!GetValue(key, &result, transaction)) {
    return false;
  }

  // 否则都可删除
  uint32_t hash = Hash(key);

  // 获取header_page
  WritePageGuard header_guard = bpm_->FetchPageWrite(header_page_id_);
  // 这里用guard.As即可，因为不需要可变数据
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();

  // 通过header_page来获取hash值对应的directory_page_id是哪一个
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  header_guard.Drop();

  // 获取directory
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  // 通过directory来获取hash值对应的bucket_page_id是哪一个
  uint32_t bucket_idx = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory->GetBucketPageId(bucket_idx);

  // 获取bucket_page
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  // 通过bucket_page删除值
  bucket_page->Remove(key, cmp_);
  bucket_guard.Drop();

  // 如果当前页被清空则需要合并
  if (bucket_page->Size() == 0) {
    if (directory->GetLocalDepth(bucket_idx) != 0) {
      bucket_idx = directory->GetSplitImageIndex(bucket_idx);
      MergeBucket(directory, bucket_idx);
    }
  }
  return true;
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
