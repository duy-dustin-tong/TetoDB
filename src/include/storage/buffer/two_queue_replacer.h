// two_queue_replacer.h

#pragma once

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>
#include "common/config.h"

namespace tetodb {

    enum class StoreType { FIFO = 0, LRU = 1 };

    struct QueueNode {
        frame_id_t fid_;
        bool is_evictable_;
        StoreType type_; 
        std::list<frame_id_t>::iterator iter_;
    };

    class TwoQueueReplacer {
    public:
        explicit TwoQueueReplacer(size_t num_frames);
        ~TwoQueueReplacer() = default;

        bool Evict(frame_id_t* frame_id);
        void RecordAccess(frame_id_t frame_id);
        void SetEvictable(frame_id_t frame_id, bool set_evictable);
        void Remove(frame_id_t frame_id);
        size_t Size();

    private:
        size_t replacer_size_;
        size_t curr_size_{ 0 };

        std::mutex latch_;


        std::list<frame_id_t> fifo_;
        std::list<frame_id_t> lru_;

        std::unordered_map<frame_id_t, QueueNode> node_store_;
    };

} // namespace tetodb