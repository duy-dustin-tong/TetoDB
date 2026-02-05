// buffer_pool_manager.h

// Role: The Cache. Moves pages from Disk <-> Memory.
// Exposes: `FetchPage(page_id)`, `NewPage(page_id*)`, `UnpinPage(page_id, is_dirty)`, `FlushPage(page_id)`.
// Consumes: DiskManager, LRUKReplacer.

#pragma once

#include <memory>
#include <vector>
#include "storage/page/page.h"


namespace tetodb {

    class BufferPoolManager {
    

    public:
        BufferPoolManager(size_t pool_size);
        ~BufferPoolManager();

        Page* FetchPage(page_id_t page_id);
        Page* NewPage(page_id_t* page_id);
        bool UnpinPage(page_id_t page_id, bool is_dirty);
        bool FlushPage(page_id_t page_id);
        bool DeletePage(page_id_t page_id);


    private:
        std::unique_ptr<char[]> data_pool_;
        std::vector<Page> pages_;
    };



} // namespace tetodb