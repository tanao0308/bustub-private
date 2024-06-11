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
	// 取得一个存储新页的空帧id
	frame_id_t frame_id;
	if(!myNewFrame(&frame_id))
	{
		return nullptr;
	}
	// 取得一个新页的id
	*page_id_ptr = AllocatePage();
	
	// 脏页写回
	myDeleteFrame(frame_id);

	// 更新信息
	mySetFrame(*page_id_ptr, frame_id);

	// 清空此帧并更新信息
	myClearFrame(*page_id_ptr, frame_id);

	return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id) -> Page * {
	// 如果在buffer中存在此页，则直接返回
	if(page_table_.find(page_id) != page_table_.end())
	{
		return &pages_[page_table_.at(page_id)];
//		frame_id = page_table_.at(page_id);
	}

	// 如果不存在，则向磁盘调度此页
	// 取得一个存储此页的空帧
	frame_id_t frame_id;
	if(!myNewFrame(&frame_id))
	{
		return nullptr;
	}
	
	// 脏页写回
	myDeleteFrame(frame_id);

	// 将此页调度到此帧上
	myPage2Frame(page_id, frame_id);

	// 更新信息
	mySetFrame(page_id, frame_id);

	return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) -> bool {
	// 检查buffer中是否存在page_id
	if(page_table_.find(page_id) == page_table_.end())
	{
		return false;
	}
	// 获取帧编号
	frame_id_t frame_id = page_table_.at(page_id);
	// 判断是否pin_count已经为0
	if(pages_[frame_id].pin_count_ == 0)
	{
		return false;
	}
	pages_[frame_id].is_dirty_ = is_dirty;
	if(--pages_[frame_id].pin_count_ == 0)
	{
		replacer_->SetEvictable(frame_id, true);
	}
	return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
	// 检查是否存在此page_id
	if(page_table_.find(page_id) == page_table_.end())
	{
		return false;
	}
	frame_id_t frame_id = page_table_.at(page_id);
	pages_[frame_id].is_dirty_ = false;

	auto promise = disk_scheduler_->CreatePromise();
	auto future = promise.get_future();
	disk_scheduler_->Schedule({/*is_write=*/true, pages_[frame_id].GetData(), /*page_id=*/pages_[frame_id].page_id_, std::move(promise)});
	future.get();

	return true;
}

void BufferPoolManager::FlushAllPages() {}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { return false; }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::myNewFrame(frame_id_t *frame_id_ptr) -> bool {
	if(!free_list_.empty())
	{
		*frame_id_ptr = free_list_.front();
		free_list_.pop_front();
		return true;
	}
	return replacer_->Evict(frame_id_ptr);
}
void BufferPoolManager::myDeleteFrame(frame_id_t frame_id) {
	// 检查此帧是否为脏帧，如果是则需要写回到硬盘
	// **一个脏页指的是已经被修改过但尚未写回到磁盘上的内存页或缓存页。
	if(pages_[frame_id].is_dirty_)
	{
		auto promise = disk_scheduler_->CreatePromise();
		auto future = promise.get_future();
		disk_scheduler_->Schedule({/*is_write=*/true, pages_[frame_id].data_, /*page_id=*/pages_[frame_id].page_id_, std::move(promise)});
		future.get();
	}
	if(pages_[frame_id].page_id_ != INVALID_PAGE_ID)
	{
		page_table_.erase(pages_[frame_id].page_id_);
		pages_[frame_id].page_id_ = INVALID_PAGE_ID;
	}
	pages_[frame_id].pin_count_ = 0;
	pages_[frame_id].is_dirty_ = false;
}
void BufferPoolManager::myPage2Frame(page_id_t page_id, frame_id_t frame_id) {
	// 将此页调度到此帧上
	auto promise = disk_scheduler_->CreatePromise();
	auto future = promise.get_future();
	disk_scheduler_->Schedule({/*is_write=*/false, pages_[frame_id].data_, /*page_id=*/page_id, std::move(promise)});
	future.get();
}
void BufferPoolManager::mySetFrame(page_id_t page_id, frame_id_t frame_id)
{
	// 更新page_id
	pages_[frame_id].page_id_ = page_id;
	// record the access history of the frame in the replacer
	replacer_->RecordAccess(frame_id);
	// pin the frame
	replacer_->SetEvictable(frame_id, false);
	pages_[frame_id].pin_count_ = 1;
	// set dirty = false
	pages_[frame_id].is_dirty_ = false;
	page_table_.insert(std::make_pair(page_id, frame_id));
}
void BufferPoolManager::myClearFrame(page_id_t page_id, frame_id_t frame_id) {
	// 清空帧内容
	pages_[frame_id].ResetMemory();
	// 设置为脏帧
	pages_[frame_id].is_dirty_ = true;
	
	FlushPage(page_id);
}
void BufferPoolManager::myTest(page_id_t page_id)
{
	std::cout<<"myTest: "<<pages_[page_id].data_<<std::endl;
	
	auto promise = disk_scheduler_->CreatePromise();
	auto future = promise.get_future();
	disk_scheduler_->Schedule({/*is_write=*/false, my_data, /*page_id=*/page_id, std::move(promise)});
	future.get();

	std::cout<<"page_id: "<<page_id<<"|2: "<<my_data<<std::endl;
}	
}// namespace bustub
