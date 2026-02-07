// table_page.h

#pragma once

#include <cstring>
#include "storage/page/page.h"
#include "storage/table/tuple.h"
#include "common/record_id.h"
#include "common/config.h" 

namespace tetodb {

    /**
     * SLOTTED PAGE HEADER LAYOUT 
     * --------------------------------------
     * | Offset | Size | Name               |
     * |--------|------|--------------------|
     * | 0      | 4    | PageId             |
     * | 4      | 4    | LSN                |
     * | 8      | 4    | PrevPageId         |
     * | 12     | 4    | NextPageId         |
     * | 16     | 4    | FreeSpacePointer   |
     * | 20     | 4    | SlotCount          |
     * --------------------------------------
     * Total Header Size: 24 Bytes
     */

    static constexpr size_t OFFSET_PAGE_ID = 0;
    static constexpr size_t OFFSET_LSN = 4;           
    static constexpr size_t OFFSET_PREV_PAGE_ID = 8;  
    static constexpr size_t OFFSET_NEXT_PAGE_ID = 12; 
    static constexpr size_t OFFSET_FREE_SPACE = 16;   
    static constexpr size_t OFFSET_SLOT_COUNT = 20;   
    static constexpr size_t SIZE_TABLE_PAGE_HEADER = 24;
    static constexpr size_t SIZE_SLOT = 8;

    class TablePage : public Page {
    public:
        void Init(page_id_t page_id, uint32_t page_size = PAGE_SIZE, page_id_t prev_page_id = INVALID_PAGE_ID, lsn_t lsn = INVALID_LSN);

        
        bool InsertTuple(const Tuple& tuple, RID* rid);
        bool MarkDelete(const RID& rid);
        bool GetTuple(const RID& rid, Tuple* tuple);

        

        inline page_id_t GetPageId() {
            return *reinterpret_cast<page_id_t*>(GetData() + OFFSET_PAGE_ID);
        }
        inline void SetPageId(page_id_t page_id) {
            *reinterpret_cast<page_id_t*>(GetData() + OFFSET_PAGE_ID) = page_id;
        }

        inline lsn_t GetLSN() {
            return *reinterpret_cast<lsn_t*>(GetData() + OFFSET_LSN);
        }
        inline void SetLSN(lsn_t lsn) {
            *reinterpret_cast<lsn_t*>(GetData() + OFFSET_LSN) = lsn;
        }

        inline page_id_t GetPrevPageId() {
            return *reinterpret_cast<page_id_t*>(GetData() + OFFSET_PREV_PAGE_ID);
        }
        inline void SetPrevPageId(page_id_t prev_page_id) {
            *reinterpret_cast<page_id_t*>(GetData() + OFFSET_PREV_PAGE_ID) = prev_page_id;
        }

        inline page_id_t GetNextPageId() {
            return *reinterpret_cast<page_id_t*>(GetData() + OFFSET_NEXT_PAGE_ID);
        }
        inline void SetNextPageId(page_id_t next_page_id) {
            *reinterpret_cast<page_id_t*>(GetData() + OFFSET_NEXT_PAGE_ID) = next_page_id;
        }

        inline uint32_t GetFreeSpacePointer() {
            return *reinterpret_cast<uint32_t*>(GetData() + OFFSET_FREE_SPACE);
        }
        inline void SetFreeSpacePointer(uint32_t ptr) {
            *reinterpret_cast<uint32_t*>(GetData() + OFFSET_FREE_SPACE) = ptr;
        }

        inline uint32_t GetSlotCount() {
            return *reinterpret_cast<uint32_t*>(GetData() + OFFSET_SLOT_COUNT);
        }
        inline void SetSlotCount(uint32_t count) {
            *reinterpret_cast<uint32_t*>(GetData() + OFFSET_SLOT_COUNT) = count;
        }

        uint32_t GetFreeSpaceRemaining();

    private:
        void SetSlot(uint32_t slot_idx, uint32_t offset, uint32_t length);
    };

} // namespace tetodb