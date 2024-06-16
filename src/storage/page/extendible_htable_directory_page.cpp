//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <cstring>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

// https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/
void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  global_depth_ = 0;
  memset(local_depths_, 0, sizeof(local_depths_));
  // 设为-1因为-1 == INVALID_PAGE_ID
  memset(bucket_page_ids_, -1, sizeof(bucket_page_ids_));
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  return bucket_idx ^ (1 << (global_depth_ - local_depths_[bucket_idx]));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return (static_cast<uint32_t>(1) << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return (static_cast<uint32_t>(1) << local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

/*
void *memcpy(void *dest, const void *src, size_t n);
        dest：指向目标内存区域的指针。
        src：指向源内存区域的指针。
        n：要复制的**字节数**，字节！
*/
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  memcpy(local_depths_ + (1 << global_depth_), local_depths_, (1 << global_depth_) * sizeof(uint8_t));
  memcpy(bucket_page_ids_ + (1 << global_depth_), bucket_page_ids_, (1 << global_depth_) * sizeof(uint32_t));
  global_depth_++;
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  global_depth_--;
  memset(local_depths_ + (1 << global_depth_), 0, (1 << global_depth_));
  memset(bucket_page_ids_ + (1 << global_depth_), -1, (1 << global_depth_));
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < (static_cast<uint32_t>(1) << global_depth_); ++i) {
    if (local_depths_[i] == global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (static_cast<uint32_t>(1) << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

/*
关于local_depth_的操作：
        local_depth_是由更上层控制的，所以只用简单地执行逻辑即可
        但是在IncrGlobalDepth的时候要同时将local_depth_也进行复制
*/
void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
