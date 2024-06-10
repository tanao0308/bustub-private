//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode { // 帧的类
 public:
  std::list<size_t> history_; // 帧的访问历史记录
  size_t k_; // 帧大小？
  frame_id_t fid_; // 帧编号 using frame_id_t = int32_t
  bool is_evictable_{false}; // 可移除标记
  
  LRUKNode(size_t current_timestamp, size_t k, frame_id_t fid, bool is_evictable=true): k_(k), fid_(fid), is_evictable_(is_evictable) {
    history_.clear();
	history_.push_back(current_timestamp);
  }
  void RecordAccess(size_t current_timestamp) {
	if(history_.size() == k_) {
	  history_.pop_front();
	}
	history_.push_back(current_timestamp);
  }
  bool operator<(const LRUKNode& other) const {
	// 传统LRU算法，用最新记录作为比较依据
	if(history_.size()<k_ && other.history_.size()<k_)
	{
		return history_.back() < other.history_.back();
	}
	else if(history_.size()<k_ && other.history_.size()==k_)
	{
		return true;
	}
	else if(history_.size()==k_ && other.history_.size()<k_)
	{
		return false;
	}
	// KLRU算法，取k位之前的历史记录作为比较依据
	else
	{
		return history_.front() < other.history_.front();
	}
  }
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  /*
  int32_t 是固定宽度的标准类型，在所有平台上都是一致的。
  unsigned int 的宽度和取值范围可能会因平台而异。

  int32_t：
      int：表示这是一个整数类型。
      32：表示这个整数类型是32位宽的。
      _t：表示这是一个类型（type）。这是C标准库中对类型命名的一种惯例，用来增强代码的可读性和一致性
  类似的有：
	int8_t, uint8_t, int16_t, uint16_t...
  */
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
   /*
   在C++中，explicit关键字主要用于构造函数和转换运算符，以防止编译器进行隐式转换，导致意外的行为。
   class Example {
   public:
       Example(int x) {
           cout << "Constructor called with " << x << endl;
       }
   };
   void func(Example ex) {
       // Some code
   }
   int main() {
       func(10); // 对Example的构造函数使用explicit可以防止这种语句的产生。
	   func(Example(10)) // 只有这样的语句才合法，使代码更清晰。
       return 0;
   }
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  // 一个将拷贝、移动构造函数和拷贝、移动运算符都设为删除的宏
  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
   * based on LRU.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   * @param access_type type of access that was received. This parameter is only needed for
   * leaderboard tests.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  /*
  size_t 的具体实现取决于平台和编译器，一般来说，它的大小是与平台上的指针大小相同的。
  在32位系统上，size_t 通常是32位；在64位系统上，size_t 通常是64位。
  
  size_t 通常用来表示内存大小和数组索引。它能够表示平台上最大可能的对象大小，因此比普通的 unsigned int 更适合用来处理涉及内存大小和数组索引的场景。
  
  用法：
    数组索引：使用 size_t 来表示数组的索引和大小。
    内存分配：使用 size_t 来指定内存分配函数（如 malloc、calloc）所需要的内存大小。
    字符串长度：使用 size_t 来表示字符串的长度。
  */
  std::unordered_map<frame_id_t, LRUKNode> node_store_; // 存储帧编号对应的帧类
  size_t current_timestamp_{0}; // 当前时间戳
  size_t num_evictable{0}; // 当前不可删除帧的数量
  size_t replacer_size_; // buffer可能的最大存储量，同时也是帧的最大id，不可有帧id >= replacer_size_
  size_t k_; // LRU_K算法中的K值
  std::mutex latch_; // 互斥锁
};

}  // namespace bustub
