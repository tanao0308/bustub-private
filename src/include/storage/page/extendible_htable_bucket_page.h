//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.h
//
// Identification: src/include/storage/page/extendible_htable_bucket_page.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * Bucket page format:
 *  ----------------------------------------------------------------------------
 * | METADATA | KEY(1) + VALUE(1) | KEY(2) + VALUE(2) | ... | KEY(n) + VALUE(n)
 *  ----------------------------------------------------------------------------
 *
 * Metadata format (size in byte, 8 bytes in total):
 *  --------------------------------
 * | CurrentSize (4) | MaxSize (4)
 *  --------------------------------
 */
#pragma once

#include <optional>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/int_comparator.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/value.h"

namespace bustub {

static constexpr uint64_t HTABLE_BUCKET_PAGE_METADATA_SIZE = sizeof(uint32_t) * 2;

constexpr auto HTableBucketArraySize(uint64_t mapping_type_size) -> uint64_t {
  return (BUSTUB_PAGE_SIZE - HTABLE_BUCKET_PAGE_METADATA_SIZE) / mapping_type_size;
};

/**
 * Bucket page for extendible hash table.

 在数据库系统中，插入操作通常不直接将具体的数据存储在桶（bucket）中，
 而是将数据的位置信息（如记录标识符 RID）存储在桶中。
 在bucket中插入的只是具体数据的位置信息而不是具体的数据。

 key: GenericKey<8>，主键的哈希值
 RID: 页号+页内偏移量
 max_size_ 指的是该桶（bucket）中最多能存储的键值对（key-value pair）的数量，具体来说就是（index_key，RID）。
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
class ExtendibleHTableBucketPage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  ExtendibleHTableBucketPage() = delete;
  DISALLOW_COPY_AND_MOVE(ExtendibleHTableBucketPage);

  /**
   * After creating a new bucket page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the bucket array
   */
  void Init(uint32_t max_size = HTableBucketArraySize(sizeof(MappingType)));

  /**
   * Lookup a key
   *
   * @param key key to lookup
   * @param[out] value value to set
   * @param cmp the comparator
   * @return true if the key and value are present, false if not found.

         如果找到指定的键，并成功设置了输出参数 value，则返回 true。
     如果未找到指定的键，则返回 false。
   */
  auto Lookup(const KeyType &key, ValueType &value, const KeyComparator &cmp) const -> bool;

  /**
   * Attempts to insert a key and value in the bucket.
   *
   * @param key key to insert
   * @param value value to insert
   * @param cmp the comparator to use
   * @return true if inserted, false if bucket is full or the same key is already present
   */
  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &cmp) -> bool;

  /**
   * Removes a key and value.
   *
   * @return true if removed, false if not found
   */
  auto Remove(const KeyType &key, const KeyComparator &cmp) -> bool;

  void RemoveAt(uint32_t bucket_idx);

  /**
   * @brief Gets the key at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the key at
   * @return key at index bucket_idx of the bucket
   */
  auto KeyAt(uint32_t bucket_idx) const -> KeyType;

  /**
   * Gets the value at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the value at
   * @return value at index bucket_idx of the bucket
   */
  auto ValueAt(uint32_t bucket_idx) const -> ValueType;

  /**
   * Gets the entry at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the entry at
   * @return entry at index bucket_idx of the bucket
   */
  auto EntryAt(uint32_t bucket_idx) const -> const std::pair<KeyType, ValueType> &;

  /**
   * @return number of entries in the bucket
   */
  auto Size() const -> uint32_t;

  /**
   * @return whether the bucket is full
   */
  auto IsFull() const -> bool;

  /**
   * @return whether the bucket is empty
   */
  auto IsEmpty() const -> bool;

  /**
   * Prints the bucket's occupancy information
   */
  void PrintBucket() const;

 private:
  uint32_t size_;
  uint32_t max_size_;
  MappingType array_[HTableBucketArraySize(sizeof(MappingType))];
};

}  // namespace bustub
