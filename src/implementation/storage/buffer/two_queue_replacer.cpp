// two_queue_replacer.cpp

#include "storage/buffer/two_queue_replacer.h"

namespace tetodb {

    TwoQueueReplacer::TwoQueueReplacer(size_t num_frames)
        : replacer_size_(num_frames) {
    }

    bool TwoQueueReplacer::Evict(frame_id_t* frame_id) {
        std::scoped_lock<std::mutex> lock(latch_);

        if (curr_size_ == 0) return false;

        // evict from FIFO first
        for (auto it = fifo_.rbegin(); it != fifo_.rend(); ++it) {
            frame_id_t frame = *it;
            if (node_store_[frame].is_evictable_) {
                *frame_id = frame;

                fifo_.erase(std::next(it).base());
                node_store_.erase(frame);
                curr_size_--;
                return true;
            }
        }

        // if FIFO is clean/pinned, try evicting from LRU (Hot data)
        for (auto it = lru_.rbegin(); it != lru_.rend(); ++it) {
            frame_id_t frame = *it;
            if (node_store_[frame].is_evictable_) {
                *frame_id = frame;

                lru_.erase(std::next(it).base());
                node_store_.erase(frame);
                curr_size_--;
                return true;
            }
        }

        return false;
    }

    void TwoQueueReplacer::RecordAccess(frame_id_t frame_id) {
        std::scoped_lock<std::mutex> lock(latch_);

        // FIX: Removed '&' to prevent dangling reference
        auto it = node_store_.find(frame_id);

        if (it == node_store_.end()) {
            fifo_.push_front(frame_id);
            node_store_[frame_id] = { frame_id, false, StoreType::FIFO, fifo_.begin() };
        }
        else {
            QueueNode& node = it->second;

            if (node.type_ == StoreType::FIFO) {
                fifo_.erase(node.iter_);

                lru_.push_front(frame_id);
                node.type_ = StoreType::LRU;
                node.iter_ = lru_.begin();
            }
            else {
                lru_.erase(node.iter_);

                lru_.push_front(frame_id);
                node.iter_ = lru_.begin();
            }
        }
    }

    void TwoQueueReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
        std::scoped_lock<std::mutex> lock(latch_);

        // FIX: Removed '&' to prevent dangling reference
        auto it = node_store_.find(frame_id);
        if (it == node_store_.end()) return;

        QueueNode& node = it->second;
        bool was_evictable = node.is_evictable_;

        if (was_evictable && !set_evictable) {
            curr_size_--;
        }
        else if (!was_evictable && set_evictable) {
            curr_size_++;
        }

        node.is_evictable_ = set_evictable;
    }

    void TwoQueueReplacer::Remove(frame_id_t frame_id) {
        std::scoped_lock<std::mutex> lock(latch_);

        // FIX: Removed '&' to prevent dangling reference
        auto it = node_store_.find(frame_id);
        if (it == node_store_.end()) return;

        QueueNode& node = it->second;

        if (!node.is_evictable_) return;

        if (node.type_ == StoreType::FIFO) {
            fifo_.erase(node.iter_);
        }
        else {
            lru_.erase(node.iter_);
        }

        node_store_.erase(frame_id);
        curr_size_--;
    }

    size_t TwoQueueReplacer::Size() {
        std::scoped_lock<std::mutex> lock(latch_);
        return curr_size_;
    }

} // namespace tetodb