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
  std::optional<frame_id_t> evicted_frame;
  size_t max_distance = 0;
  frame_id_t frame_to_evict = INVALID_FRAME_ID;

  for (const auto &[frame_id, frame_info] : node_store_) {
    if (!frame_info.is_evictable_) {
      continue;
    }

    size_t distance;
    if (frame_info.history_.size() < k_) {
      distance = SIZE_MAX;
    } else {
      auto it = frame_info.history_.end();
      std::advance(it, -k_);
      distance = frame_info.history_.back() - *it;
    }

    if (distance > max_distance ||
        (distance == max_distance && frame_info.history_.front() < node_store_[frame_to_evict].history_.front()) ||
        (distance == max_distance && frame_info.history_.front() == node_store_[frame_to_evict].history_.front() &&
         frame_info.access_type_ < node_store_[frame_to_evict].access_type_)) {
      max_distance = distance;
      frame_to_evict = frame_id;
    }
  }

  if (frame_to_evict != INVALID_FRAME_ID) {
    evicted_frame = frame_to_evict;
    node_store_.erase(frame_to_evict);
    --curr_size_;
  }

  return evicted_frame;
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
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(false, "frame id is invalid (larger than replacer_size_)");
  }

  std::lock_guard guard(latch_);
  current_timestamp_++;

  auto &node = node_store_[frame_id];
  node.history_.push_back(current_timestamp_);

  if (node.history_.size() > k_) {
    node.history_.pop_front();
  }

  // incr curr_size_ here?
  if (node.history_.size() == 1) {
    node.is_evictable_ = true;
    curr_size_++;
  }

  node.access_type_ = access_type;
  // std::cout<<"frame: "<< frame_id << " curr_size: "<< curr_size_<<std::endl;
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
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    BUSTUB_ASSERT(false, "frame id is invalid (larger than replacer_size_)");
  }

  std::lock_guard guard(latch_);

  if (auto &node = node_store_[frame_id]; node.is_evictable_ != set_evictable) {
    node.is_evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
    } else {
      curr_size_--;
    }
  }
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
  std::lock_guard guard(latch_);

  const auto it = node_store_.find(frame_id);
  if (it != node_store_.end()) {
    if (!it->second.is_evictable_) {
      return;
    }
    node_store_.erase(it);
    curr_size_--;
  }
}

/**
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
