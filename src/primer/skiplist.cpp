//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// skiplist.cpp
//
// Identification: src/primer/skiplist.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "primer/skiplist.h"
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include "common/macros.h"
#include "fmt/core.h"

namespace bustub {

/** @brief Checks whether the container is empty. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Empty() -> bool {
  this->rwlock_.lock_shared();
  auto empty = this->size_ == 0;
  this->rwlock_.unlock_shared();
  return empty;
}

/** @brief Returns the number of elements in the skip list. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Size() -> size_t {
  this->rwlock_.lock_shared();
  auto size = this->size_;
  this->rwlock_.unlock_shared();
  return size;
}

/**
 * @brief Iteratively deallocate all the nodes.
 *
 * We do this to avoid stack overflow when the skip list is large.
 *
 * If we let the compiler handle the deallocation, it will recursively call the destructor of each node,
 * which could block up the the stack.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Drop() {
  for (size_t i = 0; i < MaxHeight; i++) {
    auto curr = std::move(header_->links_[i]);
    while (curr != nullptr) {
      // std::move sets `curr` to the old value of `curr->links_[i]`,
      // and then resets `curr->links_[i]` to `nullptr`.
      curr = std::move(curr->links_[i]);
    }
  }
}

/**
 * @brief Removes all elements from the skip list.
 *
 * Note: You might want to use the provided `Drop` helper function.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Clear() {
  this->rwlock_.lock();
  Drop();
  header_ = std::make_shared<SkipNode>(MaxHeight);
  size_ = 0;
  this->rwlock_.unlock();
}

/**
 * @brief Inserts a key into the skip list.
 *
 * Note: `Insert` will not insert the key if it already exists in the skip list.
 *
 * @param key key to insert.
 * @return true if the insertion is successful, false if the key already exists.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Insert(const K &key) -> bool {
  this->rwlock_.lock();
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight);
  auto curr = header_;

  // Find the position to insert the new node
  for (int i = height_ - 1; i >= 0; i--) {
    while (curr->Next(i) != nullptr && compare_(curr->Next(i)->Key(), key)) {
      curr = curr->Next(i);
    }
    update[i] = curr;
  }

  curr = curr->Next(LOWEST_LEVEL);

  // If the key already exists, return false
  if (curr != nullptr && !compare_(key, curr->Key()) && !compare_(curr->Key(), key)) {
    this->rwlock_.unlock();
    return false;
  }

  // Generate a random height for the new node
  size_t node_height = RandomHeight();

  // If the new node height is greater than the current height, update the header
  if (node_height > height_) {
    for (size_t i = height_; i < node_height; i++) {
      update[i] = header_;
    }
    height_ = node_height;
  }

  // Create the new node
  auto new_node = std::make_shared<SkipNode>(node_height, key);

  // Insert the new node
  for (size_t i = 0; i < node_height; i++) {
    new_node->SetNext(i, update[i]->Next(i));
    update[i]->SetNext(i, new_node);
  }

  size_++;
  this->rwlock_.unlock();
  return true;
}

/**
 * @brief Erases the key from the skip list.
 *
 * @param key key to erase.
 * @return bool true if the element got erased, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Erase(const K &key) -> bool {
  this->rwlock_.lock();
  std::vector<std::shared_ptr<SkipNode>> update(MaxHeight);
  auto curr = header_;

  // Find the position of the node to be erased
  for (int i = height_ - 1; i >= 0; i--) {
    while (curr->Next(i) != nullptr && compare_(curr->Next(i)->Key(), key)) {
      curr = curr->Next(i);
    }
    update[i] = curr;
  }

  curr = curr->Next(LOWEST_LEVEL);

  // If the key does not exist, return false
  if (curr == nullptr || compare_(key, curr->Key()) || compare_(curr->Key(), key)) {
    this->rwlock_.unlock();
    return false;
  }

  // Remove the node
  for (size_t i = 0; i < height_; i++) {
    if (update[i]->Next(i) != curr) {
      break;
    }
    update[i]->SetNext(i, curr->Next(i));
  }

  // Adjust the height of the skip list if necessary
  while (height_ > 1 && header_->Next(height_ - 1) == nullptr) {
    height_--;
  }

  size_--;
  this->rwlock_.unlock();
  return true;
}

/**
 * @brief Checks whether a key exists in the skip list.
 *
 * @param key key to look up.
 * @return bool true if the element exists, false otherwise.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::Contains(const K &key) -> bool {
  // Following the standard library: Key `a` and `b` are considered equivalent if neither compares less
  // than the other: `!compare_(a, b) && !compare_(b, a)`.
  this->rwlock_.lock_shared();
  auto curr = header_;
  for (int i = height_ - 1; i >= 0; i--) {
    while (curr->Next(i) != nullptr && compare_(curr->Next(i)->Key(), key)) {
      curr = curr->Next(i);
    }
  }
  curr = curr->Next(LOWEST_LEVEL);
  auto contained = curr != nullptr && !compare_(key, curr->Key()) && !compare_(curr->Key(), key);
  this->rwlock_.unlock_shared();
  return contained;
}

/**
 * @brief Prints the skip list for debugging purposes.
 *
 * Note: You may modify the functions in any way and the output is not tested.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::Print() {
  auto node = header_->Next(LOWEST_LEVEL);
  while (node != nullptr) {
    fmt::println("Node {{ key: {}, height: {} }}", node->Key(), node->Height());
    node = node->Next(LOWEST_LEVEL);
  }
}

/**
 * @brief Generate a random height. The height should be cappped at `MaxHeight`.
 * Note: we implement/simulate the geometric process to ensure platform independence.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::RandomHeight() -> size_t {
  // Branching factor (1 in 4 chance), see Pugh's paper.
  static constexpr unsigned int branching_factor = 4;
  // Start with the minimum height
  size_t height = 1;
  while (height < MaxHeight && (rng_() % branching_factor == 0)) {
    height++;
  }
  return height;
}

/**
 * @brief Gets the current node height.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Height() const -> size_t {
  // The height of a node is determined by its number of forward links.
  return links_.size();
}

/**
 * @brief Gets the next node by following the link at `level`.
 *
 * @param level index to the link.
 * @return std::shared_ptr<SkipNode> the next node, or `nullptr` if such node does not exist.
 */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Next(size_t level) const
    -> std::shared_ptr<SkipNode> {
  if (level < links_.size()) {
    return links_[level];
  }
  return nullptr;
}

/**
 * @brief Set the `node` to be linked at `level`.
 *
 * @param level index to the link.
 */
SKIPLIST_TEMPLATE_ARGUMENTS void SkipList<K, Compare, MaxHeight, Seed>::SkipNode::SetNext(
    size_t level, const std::shared_ptr<SkipNode> &node) {
  if (level < links_.size()) {
    links_[level] = node;
  }
}

/** @brief Returns a reference to the key stored in the node. */
SKIPLIST_TEMPLATE_ARGUMENTS auto SkipList<K, Compare, MaxHeight, Seed>::SkipNode::Key() const -> const K & {
  return key_;
}

// Below are explicit instantiation of template classes.
template class SkipList<int>;
template class SkipList<std::string>;
template class SkipList<int, std::greater<>>;
template class SkipList<int, std::less<>, 8>;

}  // namespace bustub
