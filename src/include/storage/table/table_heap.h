// table_heap.h

#pragma once

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/page/table_page.h"
#include "storage/table/tuple.h"


namespace tetodb {

    class Transaction; // Forward declaration (we aren't using it yet)


    class TableHeap {
    public:
        
        TableHeap(BufferPoolManager* bpm, Transaction* txn = nullptr);

        TableHeap(BufferPoolManager* bpm, page_id_t first_page_id);

      
        bool InsertTuple(const Tuple& tuple, RID* rid, Transaction* txn = nullptr);

        bool GetTuple(const RID& rid, Tuple* tuple, Transaction* txn = nullptr);

        bool MarkDelete(const RID& rid, Transaction* txn = nullptr);

        // Updates a tuple.
        // Note: If the new tuple is larger, this might change the RID (Delete + Insert).
        bool UpdateTuple(const Tuple& tuple, RID* rid, Transaction* txn = nullptr);

        inline page_id_t GetFirstPageId() { return first_page_id_; }

    private:
        BufferPoolManager* bpm_;
        page_id_t first_page_id_;
        page_id_t last_page_id_;
    };

} // namespace tetodb