// table_iterator.h
#pragma once

#include "common/record_id.h"

namespace tetodb {

    class TableHeap;

    class TableIterator {
    public:
        // Constructor: Points to a specific position (RID)
        TableIterator(TableHeap* table_heap, RID rid);

        // We remove operator* and operator-> because we no longer cache the tuple.
        // Instead, we just expose the current RID.
        inline RID GetRid() const {
            return rid_;
        }

        // Increment (Prefix: ++it)
        // Inside your .cpp, this should scan the page to find the NEXT valid RID,
        // and update rid_. It should NOT fetch the tuple payload.
        TableIterator& operator++();

        // Increment (Postfix: it++)
        TableIterator operator++(int);

        // Comparison
        inline bool operator==(const TableIterator& itr) const {
            return rid_ == itr.rid_;
        }

        inline bool operator!=(const TableIterator& itr) const {
            return !(*this == itr);
        }

    private:
        TableHeap* table_heap_;
        RID rid_;

        // REMOVED: Tuple tuple_;
    };

} // namespace tetodb