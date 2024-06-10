//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // 启动后台线程（std::optional.emplace()方式启动lambda函数）
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  std::optional<DiskRequest> opt_r = std::move(r);
  request_queue_.Put(std::move(opt_r));
}

// 线程全生命周期都在此函数内
void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> disk_request;
  while ((disk_request = request_queue_.Get()).has_value()) {
    // 处理写事件逻辑
    if (disk_request->is_write_) {
      disk_manager_->WritePage(disk_request->page_id_, disk_request->data_);
    }
    // 处理读事件逻辑
    else {
      disk_manager_->ReadPage(disk_request->page_id_, disk_request->data_);
    }
    disk_request->callback_.set_value(true);
  }
}

}  // namespace bustub
