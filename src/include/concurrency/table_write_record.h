// table_write_record.h

#pragma once

#include "common/record_id.h"
#include "storage/table/tuple.h"


namespace tetodb {

    class TableHeap;


    // The type of write operation
    enum class WType { INSERT = 0, DELETE, UPDATE };

    /**
     * TableWriteRecord represents a single modification to a TableHeap.
     * It contains all the information necessary to UNDO the operation.
     */
    class TableWriteRecord {
    public:
        // --- ADDED: Explicit Default Constructor ---
        TableWriteRecord() = default;

        // Constructor for INSERT operations (we don't need the old tuple, just the RID to delete it)
        TableWriteRecord(RID rid, WType wtype, TableHeap* table_heap)
            : rid_(rid), wtype_(wtype), table_heap_(table_heap) {
        }

        // Constructor for DELETE/UPDATE operations (we need to remember the old tuple to restore it)
        TableWriteRecord(RID rid, WType wtype, const Tuple& tuple, TableHeap* table_heap)
            : rid_(rid), wtype_(wtype), tuple_(tuple), table_heap_(table_heap) {
        }

        RID rid_;
        WType wtype_;
        Tuple tuple_; // The OLD data (only populated for Delete/Update)
        TableHeap* table_heap_; // The specific table this happened in
    };

} // namespace tetodb