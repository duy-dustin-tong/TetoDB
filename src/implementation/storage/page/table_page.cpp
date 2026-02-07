// table_page.cpp

#include "storage/page/table_page.h"

namespace tetodb {

    void TablePage::Init(page_id_t page_id, uint32_t page_size, page_id_t prev_page_id, lsn_t lsn) {
        SetPageId(page_id);
        SetLSN(lsn); 
        SetPrevPageId(prev_page_id);
        SetNextPageId(INVALID_PAGE_ID);

        SetFreeSpacePointer(page_size);
        SetSlotCount(0);
    }

    bool TablePage::InsertTuple(const Tuple& tuple, RID* rid) {
        uint32_t tuple_size = tuple.GetSize();

        if (GetFreeSpaceRemaining() < tuple_size + SIZE_SLOT) {
            return false;
        }

        uint32_t free_ptr = GetFreeSpacePointer();
        uint32_t slot_count = GetSlotCount();

        uint32_t new_free_ptr = free_ptr - tuple_size;
        memcpy(GetData() + new_free_ptr, tuple.GetData(), tuple_size);

        SetSlot(slot_count, new_free_ptr, tuple_size);

        // 5. Update Header
        SetFreeSpacePointer(new_free_ptr);
        SetSlotCount(slot_count + 1);

        // 6. Return the new RID
        rid->Set(GetPageId(), slot_count);
        return true;
    }

    bool TablePage::MarkDelete(const RID& rid) {
        uint32_t slot_idx = rid.GetSlotId();

        // Bounds check
        if (slot_idx >= GetSlotCount()) {
            return false;
        }

        // To delete, we set the Tuple Length to 0 (Tombstone).
        // Slot Format: [Offset (4B)] [Length (4B)]
        uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);

        // Zero out the length field (offset + 4)
        uint32_t zero = 0;
        memcpy(GetData() + slot_offset + 4, &zero, 4);

        return true;
    }

    bool TablePage::GetTuple(const RID& rid, Tuple* tuple) {
        uint32_t slot_idx = rid.GetSlotId();

        // Bounds check
        if (slot_idx >= GetSlotCount()) {
            return false;
        }

        // Read Slot Info
        uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
        uint32_t tuple_offset = *reinterpret_cast<uint32_t*>(GetData() + slot_offset);
        uint32_t tuple_size = *reinterpret_cast<uint32_t*>(GetData() + slot_offset + 4);

        // Check if deleted (Tombstone)
        if (tuple_size == 0) {
            return false;
        }

        // Read the actual data from the offset
        tuple->DeserializeFrom(GetData() + tuple_offset, tuple_size);
        tuple->SetRid(rid);

        return true;
    }
   
    void TablePage::SetSlot(uint32_t slot_idx, uint32_t offset, uint32_t length) {
        uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
        *reinterpret_cast<uint32_t*>(GetData() + slot_offset) = offset;
        *reinterpret_cast<uint32_t*>(GetData() + slot_offset + 4) = length;
    }

    uint32_t TablePage::GetFreeSpaceRemaining() {
        uint32_t free_ptr = GetFreeSpacePointer();
        uint32_t header_end = SIZE_TABLE_PAGE_HEADER + (GetSlotCount() * SIZE_SLOT);

        if (header_end > free_ptr) {
            // Should not happen, implies corruption
            return 0;
        }
        return free_ptr - header_end;
    }

} // namespace tetodb