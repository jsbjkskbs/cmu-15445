//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include "fmt/ostream.h"

namespace bustub {

/**
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 * @param k the number of historical references to track for each frame
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard guard(latch_);

  if (curr_size_ == 0) {
    return std::nullopt;
  }

  if (!less_than_k_list_.empty()) {
    auto it = less_than_k_list_.begin();
    while (it != less_than_k_list_.end()) {
      const auto &node = *it;
      if (!node->is_evictable_) {
        ++it;
        continue;
      }
      less_than_k_map_.erase(node->fid_);
      less_than_k_list_.erase(it);
      curr_size_--;
      // fmt::println("evicted: {}", node->fid_);
      PrintFrames();
      return node->fid_;
    }
  }

  if (!greater_equal_k_list_.empty()) {
    auto it = greater_equal_k_list_.begin();
    while (it != greater_equal_k_list_.end()) {
      const auto &node = *it;
      if (!node->is_evictable_) {
        ++it;
        continue;
      }
      greater_equal_k_map_.erase(node->fid_);
      greater_equal_k_list_.erase(it);
      curr_size_--;
      // fmt::println("evicted: {}", node->fid_);
      PrintFrames();
      return node->fid_;
    }
  }

  PrintFrames();
  return std::nullopt;
}

/**
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
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard guard(latch_);

  if (frame_id >= static_cast<frame_id_t>(replacer_size_) || frame_id < 0) {
    BUSTUB_ASSERT("frame id {} is invalid (larger than replacer_size_)", frame_id);
  }
  current_timestamp_++;

  const auto ls_it = less_than_k_map_.find(frame_id);
  const auto ge_it = greater_equal_k_map_.find(frame_id);
  if (ls_it == less_than_k_map_.end() && ge_it == greater_equal_k_map_.end()) {
    const auto node = std::make_shared<LRUKNode>();
    node->fid_ = frame_id;
    node->k_ = k_;
    node->history_.push_back(current_timestamp_);

    less_than_k_map_[frame_id] = node;
    less_than_k_list_.push_back(node);
  } else {
    if (ls_it != less_than_k_map_.end()) {
      const auto node = ls_it->second;
      node->history_.push_back(current_timestamp_);

      if (node->history_.size() >= k_) {
        node->UpdateLastKTimestamp();
        less_than_k_map_.erase(frame_id);
        less_than_k_list_.remove(node);
        InsertGENode(node);
      }
    } else if (ge_it != greater_equal_k_map_.end()) {
      const auto node = ge_it->second;
      node->history_.push_back(current_timestamp_);
      node->UpdateLastKTimestamp();
      greater_equal_k_map_.erase(frame_id);
      greater_equal_k_list_.remove(node);
      InsertGENode(node);
    }
  }
  // fmt::println("Record Access: {}", frame_id);
  PrintFrames();
  // std::cout<<"frame: "<< frame_id << " curr_size: "<< curr_size_<<std::endl;
}

void LRUKReplacer::InsertGENode(const std::shared_ptr<LRUKNode> &node) {
  node->PrintTimestamp();
  greater_equal_k_map_[node->fid_] = node;
  if (greater_equal_k_list_.empty()) {
    greater_equal_k_list_.push_back(node);
    return;
  }
  auto it = greater_equal_k_list_.begin();
  while (it != greater_equal_k_list_.end() && node->last_k_timestamp > (*it)->last_k_timestamp) {
    ++it;
  }
  greater_equal_k_list_.insert(it, node);
}

/**
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
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_) || frame_id < 0) {
    BUSTUB_ASSERT(false, "frame id is invalid (larger than replacer_size_)");
  }

  std::lock_guard guard(latch_);

  const auto ls_it = less_than_k_map_.find(frame_id);
  const auto ge_it = greater_equal_k_map_.find(frame_id);

  if (ls_it == less_than_k_map_.end() && ge_it == greater_equal_k_map_.end()) {
    // throw std::invalid_argument(fmt::format("frame id {} is invalid", frame_id));
    return;
  }

  const auto node = ls_it != less_than_k_map_.end() ? ls_it->second : ge_it->second;
  if (node->is_evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!node->is_evictable_ && set_evictable) {
    curr_size_++;
  }
  node->is_evictable_ = set_evictable;
}

/**
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
void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id > static_cast<frame_id_t>(replacer_size_) || frame_id < 0) {
    BUSTUB_ASSERT(false, "frame id is invalid (larger than replacer_size_)");
  }

  std::lock_guard guard(latch_);

  auto ls_it = less_than_k_map_.find(frame_id);
  auto ge_it = greater_equal_k_map_.find(frame_id);
  if (ls_it == less_than_k_map_.end() && ge_it == greater_equal_k_map_.end()) {
    return;
  }

  if (ls_it != less_than_k_map_.end()) {
    const auto node = less_than_k_map_[frame_id];
    if (!node->is_evictable_) {
      throw std::logic_error(fmt::format("frame id {} is not evictable", frame_id));
    }
    less_than_k_map_.erase(ls_it);
    less_than_k_list_.remove(node);
  } else if (ge_it != greater_equal_k_map_.end()) {
    const auto node = greater_equal_k_map_[frame_id];
    if (!node->is_evictable_) {
      throw std::logic_error(fmt::format("frame id {} is not evictable", frame_id));
    }
    greater_equal_k_map_.erase(ge_it);
    greater_equal_k_list_.remove(node);
  }

  curr_size_--;
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard guard(latch_);
  return curr_size_;
}

}  // namespace bustub
