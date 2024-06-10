//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
	node_store_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
	
	std::lock_guard<std::mutex> lock(latch_);

	LRUKNode const *node_ptr = nullptr;
	for(const auto& iter : node_store_)
	{
		if(iter.second.is_evictable_ == 0)
		{
			continue;
		}
		if(node_ptr == nullptr || iter.second < *(node_ptr))
		{
			node_ptr = &(iter.second);
		}
	}
	if(node_ptr != nullptr)
	{
		*frame_id = node_ptr->fid_;
		node_store_.erase(node_ptr->fid_);
	}
	return node_ptr != nullptr ? true : false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {

	std::lock_guard<std::mutex> lock(latch_);

	// 检查帧编号是否合法
	if(frame_id<0 || frame_id >=(frame_id_t)replacer_size_)
	{
		throw bustub::Exception("frame id is invalid in Record Access");
	}

	current_timestamp_++;
//	std::cout<<"current timestamp = "<<current_timestamp_<<std::endl;
	if(node_store_.find(frame_id) == node_store_.end())
	{
		node_store_.emplace(frame_id, LRUKNode(current_timestamp_, k_, frame_id));
	}
	else
	{
		node_store_.at(frame_id).RecordAccess(current_timestamp_);
	}
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

	std::lock_guard<std::mutex> lock(latch_);

	if(node_store_.find(frame_id) == node_store_.end())
	{
		throw bustub::Exception("frame id is invalid in Set Evictable");
	}
	LRUKNode &node = node_store_.at(frame_id);
//	std::cout<<"evictable: "<<set_evictable<<" "<<node.is_evictable_<<std::endl;
	num_evictable += (size_t)node.is_evictable_ - (size_t)set_evictable;
	node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {

	std::lock_guard<std::mutex> lock(latch_);

	// 如果没有此帧则立即返回
	if(node_store_.find(frame_id) == node_store_.end())
	{
		return;
	}
	// 如果试图删除标记为不可删除的帧，则抛出报错
	if(!node_store_.at(frame_id).is_evictable_)
	{
		throw bustub::Exception("can't remove unevictable frame");
	}
	// 正确删除
	node_store_.erase(frame_id);
}

/*
buffer内可移除的帧的数量，不超过replacer_size_
*/
auto LRUKReplacer::Size() -> size_t {
	return node_store_.size() - num_evictable;
}

}  // namespace bustub
