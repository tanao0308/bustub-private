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

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = 0;
  global_depth_ = 0;
  memset(local_depths_, 0, sizeof(local_depths_));
  // 设为-1因为-1 == INVALID_PAGE_ID
  memset(bucket_page_ids_, -1, sizeof(bucket_page_ids_));
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  if (global_depth_ == 0) {
    return 0;
  }
  return hash >> (sizeof(hash) * 8 - global_depth_);
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_page_ids_[bucket_idx] == -1) {
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { return 0; }

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  return static_cast<uint32_t>((1 << global_depth_) - 1);
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  return static_cast<uint32_t>((1 << local_depths_[bucket_idx]) - 1);
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() { global_depth_++; }

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

// todo
auto ExtendibleHTableDirectoryPage::CanShrink() -> bool { return false; }

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (static_cast<uint32_t>(1) << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
