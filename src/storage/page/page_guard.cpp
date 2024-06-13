#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
    : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
  that.Reset();
}

void BasicPageGuard::Drop() {
  if (bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
  }
  Reset();
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
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
  if (bpm_ != nullptr) {
    bpm_->UnpinPage(PageId(), is_dirty_);
    Reset();
  }
  // 相比于Drop少了三行，因为会自动清理
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  ReadPageGuard rpg = ReadPageGuard(bpm_, page_);
  Reset();
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  WritePageGuard wpg = WritePageGuard(bpm_, page_);
  Reset();
  return wpg;
}

void BasicPageGuard::Reset() {
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { return *this; }

void ReadPageGuard::Drop() {}

ReadPageGuard::~ReadPageGuard() {}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { return *this; }

void WritePageGuard::Drop() {}

WritePageGuard::~WritePageGuard() {}  // NOLINT

}  // namespace bustub
