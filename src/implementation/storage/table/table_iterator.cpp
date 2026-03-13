// table_iterator.cpp

#include "storage/table/table_iterator.h"
#include "storage/table/table_heap.h"
#include "storage/page/page_guard.h"

namespace tetodb {

    TableIterator::TableIterator(TableHeap* table_heap, RID rid)
        : table_heap_(table_heap), rid_(rid) {

        // Valid start? Check if the specific RID is actually valid.
        if (rid_.GetPageId() != INVALID_PAGE_ID) {
            BufferPoolManager* bpm = table_heap_->GetBufferPoolManager();
            Page* page = bpm->FetchPage(rid_.GetPageId());

            if (page != nullptr) {
                ReadPageGuard guard(bpm, page);
                auto table_page = guard.As<TablePage>();

                // Use a proper metadata check instead of a dummy fetch
                if (!table_page->IsValidTuple(rid_.GetSlotId())) {
                    guard.Drop(); // Unlatch before moving forward
                    ++(*this);    // Move to the next valid one
                }
            }
        }
    }

    TableIterator& TableIterator::operator++() {
        // 1. Stop if already at end
        if (rid_.GetPageId() == INVALID_PAGE_ID) {
            return *this;
        }

        // 2. Advance to the NEXT slot immediately.
        rid_.Set(rid_.GetPageId(), rid_.GetSlotId() + 1);

        BufferPoolManager* bpm = table_heap_->GetBufferPoolManager();

        // 3. Loop until we find a valid tuple or run out of pages
        while (rid_.GetPageId() != INVALID_PAGE_ID) {

            Page* page = bpm->FetchPage(rid_.GetPageId());
            if (page == nullptr) {
                rid_.Set(INVALID_PAGE_ID, 0);
                return *this;
            }

            ReadPageGuard guard(bpm, page);
            auto table_page = guard.As<TablePage>();

            // Check slots starting from the current rid_.GetSlotId()
            while (rid_.GetSlotId() < table_page->GetSlotCount()) {

                // PROPER CHECK: Just read the slot metadata
                if (table_page->IsValidTuple(rid_.GetSlotId())) {
                    // FOUND IT! Guard destructor unpins the page automatically.
                    return *this;
                }

                // Slot empty or deleted? Move to next slot.
                rid_.Set(rid_.GetPageId(), rid_.GetSlotId() + 1);
            }

            // End of Page reached. Jump to next page.
            page_id_t next_page_id = table_page->GetNextPageId();
            rid_.Set(next_page_id, 0);

            // Loop repeats. Next iteration starts checking at Slot 0.
        }

        return *this;
    }

    TableIterator TableIterator::operator++(int) {
        TableIterator temp = *this;
        ++(*this);
        return temp;
    }

} // namespace tetodb