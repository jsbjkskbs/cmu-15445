//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "fmt/printf.h"

#include <iostream>
#include <memory>

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

// #define DEBUG_FOR_LRUK

class LRUKNode {
 public:
  void UpdateLastKTimestamp() {
    if (history_.size() >= k_) {
      auto it = history_.rbegin();
      std::advance(it, k_ - 1);
      last_k_timestamp = *it;
    } else {
      last_k_timestamp = history_.front();
    }
#ifdef DEBUG_FOR_LRUK
      fmt::println("[DEBUG] fid: {}, k: {}, last_k_timestamp: {}", fid_, k_, last_k_timestamp);
#endif
  }

  void PrintTimestamp() {
#ifdef DEBUG_FOR_LRUK
    fmt::print("fid: {}, ts: [", fid_);
    for (auto ts : history_) {
      fmt::print("{} ", ts);
    }
    fmt::println("]");
#endif
  }

  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.

  std::list<size_t> history_;
  size_t k_;
  frame_id_t fid_;
  bool is_evictable_{false};

  size_t last_k_timestamp{0};
  AccessType access_type_{AccessType::Unknown};
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
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() {
    std::lock_guard guard(latch_);
    // node_store_.clear();
  }

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

  void InsertGENode(const std::shared_ptr<LRUKNode> &node);

  void PrintFrames() const {
#ifdef DEBUG_FOR_LRUK
    fmt::println("Less Than K List:");
    for (const auto &fid : less_than_k_list_) {
      if (less_than_k_map_.count(fid->fid_) == 0) {
        fmt::println("{} missing in less than K map\n", fid->fid_);
        return;
      }
      std::cout << fid->fid_ << (fid->is_evictable_ ? " " : "! ");
    }

    fmt::println("\nGreater Equal K List:");
    for (const auto &fid : greater_equal_k_list_) {
      if (greater_equal_k_map_.count(fid->fid_) == 0) {
        fmt::println("{} missing greater than K map\n", fid->fid_);
        return;
      }
      std::cout << fid->fid_ << (fid->is_evictable_ ? " " : "! ");
    }
    fmt::println("\n");
#endif
  }

 private:
  // Remove maybe_unused if you start using them.
  // std::unordered_map<frame_id_t, LRUKNode> node_store_;
  size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;

  std::list<std::shared_ptr<LRUKNode>> less_than_k_list_;
  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> less_than_k_map_;

  std::list<std::shared_ptr<LRUKNode>> greater_equal_k_list_;
  std::unordered_map<frame_id_t, std::shared_ptr<LRUKNode>> greater_equal_k_map_;
};

}  // namespace bustub
