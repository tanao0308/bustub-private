//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // disk_scheduler_在构造函数内就开启了线程，所以无需手动开启
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  free_list_.clear();

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id_ptr) -> Page * {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 取得一个存储新页的空帧id
  frame_id_t frame_id;
  if (!MyNewFrame(&frame_id)) {
    return nullptr;
  }
  // 取得一个新页的id
  *page_id_ptr = AllocatePage();
  // 将新页写到帧里
  MyPage2Frame(*page_id_ptr, frame_id);
  // 清空此帧
  MyClearFrame(*page_id_ptr, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  frame_id_t frame_id;
  // 如果在buffer中存在此页，则直接返回
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id = page_table_.at(page_id);
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[page_table_.at(page_id)];
  }

  // 如果不存在，则向磁盘调度此页
  // 取得一个存储此页的空帧
  if (!MyNewFrame(&frame_id)) {
    return nullptr;
  }
  // 将此页调度到此帧上
  MyPage2Frame(page_id, frame_id);
  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 检查buffer中是否存在page_id
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id;
  // 获取帧编号
  frame_id = page_table_.at(page_id);
  // 判断是否pin_count已经为0
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  // 开始unpin
  pages_[frame_id].is_dirty_ |= is_dirty;
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 检查是否存在此page_id
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id;
  frame_id = page_table_.at(page_id);

  // 开始写回
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, pages_[frame_id].data_, /*page_id=*/page_id, std::move(promise)});
  future.get();

  // 更新
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {  // untested
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  for (auto page_frame : page_table_) {
    FlushPage(page_frame.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 若缓冲区不存在该页，则直接返回true
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id;
  frame_id = page_table_.at(page_id);
  // 若不能删除
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }

  // 正式删除
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  // reset memory and meta
  MyReset(frame_id);

  {
    //    std::lock_guard<std::mutex> lock(latch_);
    DeallocatePage(page_id);
  }
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/true, init_data_, /*page_id=*/next_page_id_, std::move(promise)});
  future.get();
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::MyNewFrame(frame_id_t *frame_id_ptr) -> bool {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 选取新帧
  if (!free_list_.empty()) {
    *frame_id_ptr = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(frame_id_ptr)) {
    return false;
  }

  // 检查此帧是否为脏帧，如果是则需要写回到硬盘
  // **一个脏页指的是已经被修改过但尚未写回到磁盘上的内存页或缓存页。
  if (pages_[*frame_id_ptr].page_id_ != INVALID_PAGE_ID) {
    if (pages_[*frame_id_ptr].is_dirty_) {
      FlushPage(pages_[*frame_id_ptr].page_id_);
    }
    page_table_.erase(pages_[*frame_id_ptr].page_id_);
    MyReset(*frame_id_ptr);
  }
  return true;
}
void BufferPoolManager::MyPage2Frame(page_id_t page_id, frame_id_t frame_id) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 将此页调度到此帧上
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({/*is_write=*/false, pages_[frame_id].data_, /*page_id=*/page_id, std::move(promise)});
  future.get();

  // 更新page_id
  pages_[frame_id].page_id_ = page_id;
  // record the access history of the frame in the replacer
  replacer_->RecordAccess(frame_id);
  // pin the frame
  replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].pin_count_ = 1;
  // set dirty = false
  //  pages_[frame_id].is_dirty_ = false;
  page_table_.insert(std::make_pair(page_id, frame_id));
}
void BufferPoolManager::MyClearFrame(page_id_t page_id, frame_id_t frame_id) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  // 清空帧内容
  pages_[frame_id].ResetMemory();
}
void BufferPoolManager::MyReset(frame_id_t frame_id) {
  std::lock_guard<std::recursive_mutex> lock(mutex_);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
}
}  // namespace bustub
