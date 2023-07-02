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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  frame_id_t fid = -1;
  frame_id_t inf_fid = -1;
  size_t ts = current_timestamp_ + 1;
  size_t inf_ts = current_timestamp_ + 1;
  for (auto it : node_store_) {
    if (!it.second.is_evictable_) {  // skip non-evictable
      continue;
    }
    if (it.second.history_.size() < k_) {  // inf
      if (it.second.history_.empty()) {
        inf_fid = it.first;
        break;
      }
      if (it.second.history_.front() < inf_ts) {
        inf_fid = it.first;
        inf_ts = it.second.history_.front();
      }
      continue;
    }
    if (inf_fid != -1) {  // inf already found, skip
      continue;
    }
    // common case, no inf, we compare front directly
    if (it.second.history_.front() < ts) {
      fid = it.first;
      ts = it.second.history_.front();
    }
  }
  if (inf_fid != -1) {
    *frame_id = inf_fid;
    Remove(inf_fid);
    return true;
  }
  if (fid != -1) {
    *frame_id = fid;
    Remove(fid);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("invlaid frame_id");
  }
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    node_store_[frame_id] = LRUKNode();
    iter = node_store_.find(frame_id);
  }
  iter->second.history_.push_front(++current_timestamp_);
  if (iter->second.history_.size() > k_) {
    iter->second.history_.pop_back();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || frame_id >= static_cast<frame_id_t>(replacer_size_)) {
    throw Exception("invlaid frame_id");
  }
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  if (!iter->second.is_evictable_ && set_evictable) {
    ++curr_size_;
    iter->second.is_evictable_ = set_evictable;
  } else if (iter->second.is_evictable_ && !set_evictable) {
    --curr_size_;
    iter->second.is_evictable_ = set_evictable;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }
  if (!iter->second.is_evictable_) {
    throw Exception("frame is not evictable");
  }
  iter->second.history_.clear();
  iter->second.is_evictable_ = false;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
