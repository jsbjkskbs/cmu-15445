//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "fmt/base.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

/**
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
void DiskScheduler::StartWorkerThread() {
  std::optional<DiskRequest> request;

  while ((request = request_queue_.Get()) != std::nullopt) {
    if (request->cond_.has_value()) {
      request->cond_.value().get();
    }
    if (request->is_write_) {
      // fmt::println("write data, page_id: {}, data: {}", request->page_id_, request->data_);
      disk_manager_->WritePage(request->page_id_, request->data_);
    } else {
      // fmt::println("read data, page_id: {}, data: {}", request->page_id_, request->data_);
      disk_manager_->ReadPage(request->page_id_, request->data_);
    }
    request->callback_.set_value(true);
  }
}

}  // namespace bustub
