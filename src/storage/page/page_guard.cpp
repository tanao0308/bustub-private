// https://blog.csdn.net/qq_28625359/article/details/137108932
#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  that.Reset();
}

void BasicPageGuard::Drop() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  if (page_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  Reset();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  if (this != &that) {
    // 这里需要先Drop()，因为此时本管理器相当于删除了
    Drop();

    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;

    that.Reset();
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  Drop();
};  // NOLINT

// 这个函数需要在转换之后对产生的ReadPageGuard进行加锁
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  ReadPageGuard rpg = ReadPageGuard(bpm_, page_);
  page_->RLatch();
  Reset();
  return rpg;
}

// 同上
auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  WritePageGuard wpg = WritePageGuard(bpm_, page_);
  page_->WLatch();
  Reset();
  return wpg;
}

void BasicPageGuard::Reset() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (page_ == nullptr ? -1 : PageId()),
            (page_ == nullptr ? -1 : page_->GetPinCount()));
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

// ----------ReadPageGuard-----------
// 移动构造函数默认即可，默认是递归进行移动构造
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

// 移动赋值运算符，需要对原来的page解锁，再Drop
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->RUnlatch();
      guard_.Drop();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

// 同样是先解锁再Drop
void ReadPageGuard::Drop() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  if (guard_.page_ != nullptr) {
    guard_.page_->RUnlatch();
    guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  Drop();
}  // NOLINT

// ----------WritePageGuard-----------
// 参考Read的实现
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  if (this != &that) {
    if (guard_.page_ != nullptr) {
      guard_.page_->WUnlatch();
      guard_.Drop();
    }
    guard_ = std::move(that.guard_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
    guard_.Drop();
  }
}
WritePageGuard::~WritePageGuard() {
  LOG_DEBUG("PageId: %d, PinCount: %d", (guard_.page_ == nullptr ? -1 : PageId()),
            (guard_.page_ == nullptr ? -1 : guard_.page_->GetPinCount()));
  if (guard_.page_ != nullptr) {
    guard_.page_->WUnlatch();
  }
}  // NOLINT
}  // namespace bustub
