// buffer_pool_manager.h

// Role: The Cache. Moves pages from Disk <-> Memory.
// Exposes: `FetchPage(page_id)`, `NewPage(page_id*)`, `UnpinPage(page_id, is_dirty)`, `FlushPage(page_id)`.
// Consumes: DiskManager, LRUKReplacer.

#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <list>

#ifdef _WIN32
#include <malloc.h>
#endif

#include "storage/page/page.h"
#include "storage/disk/disk_manager.h"
#include "storage/buffer/two_queue_replacer.h"


namespace tetodb {

    struct AlignedDeleter {
        void operator()(char* ptr) const {
        #ifdef _WIN32
            _aligned_free(ptr);
        #else
            free(ptr); // on Linux, posix_memalign pointers can be freed with free()
        #endif
        }
    };


    class BufferPoolManager {
    

    public:
        BufferPoolManager(size_t pool_size, DiskManager* disk_manager, TwoQueueReplacer* replacer);
        ~BufferPoolManager();

        Page* FetchPage(page_id_t page_id);
        Page* NewPage(page_id_t* page_id);
        bool UnpinPage(page_id_t page_id, bool is_dirty);
        bool FlushPage(page_id_t page_id);
        bool DeletePage(page_id_t page_id);
        void FlushAllPages();
        


    private:
        bool GetFreeFrame(frame_id_t* frame_id);

    private:
        std::mutex latch_;

        std::unique_ptr<char[], AlignedDeleter> data_pool_;
        std::vector<Page> pages_;

        std::unordered_map<page_id_t, frame_id_t> page_table_;
        std::list<frame_id_t> free_list_;

        DiskManager* disk_manager_;
        TwoQueueReplacer* replacer_;

        size_t pool_size_;

    };



} // namespace tetodb