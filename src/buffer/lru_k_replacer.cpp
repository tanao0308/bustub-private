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

/*
replacer_size_ = 初始大小 - 不能被移除的帧数量
*/
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
	node_store_.clear();
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {

	return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
	if(node_store_.find(frame_id) == node_store_.end())
	{
		node_store_.emplace(current_timestamp_, frame_id, LRUKNode(k_, fid));
	}
	else
	{
		node_store_.at(frame_id).add_log(current_timestamp_);
	}
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {

}

void LRUKReplacer::Remove(frame_id_t frame_id) {

}

/*
buffer内可移除的帧的数量，不超过replacer_size_
*/
auto LRUKReplacer::Size() -> size_t {
	return curr_size_;
	return 0;
}

}  // namespace bustub
