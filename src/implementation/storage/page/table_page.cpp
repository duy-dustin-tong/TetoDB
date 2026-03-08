// table_page.cpp

#include "storage/page/table_page.h"

namespace tetodb {

void TablePage::Init(page_id_t page_id, uint32_t page_size,
                     page_id_t prev_page_id, lsn_t lsn) {
  SetPageId(page_id);
  SetLSN(lsn);
  SetPrevPageId(prev_page_id);
  SetNextPageId(INVALID_PAGE_ID);

  SetFreeSpacePointer(page_size);
  SetSlotCount(0);
}

void TablePage::Compact() {
  char temp_page[PAGE_SIZE];
  std::memset(temp_page, 0, PAGE_SIZE);
  memcpy(temp_page, GetData(), SIZE_TABLE_PAGE_HEADER);

  uint32_t slot_count = GetSlotCount();
  memcpy(temp_page + SIZE_TABLE_PAGE_HEADER, GetData() + SIZE_TABLE_PAGE_HEADER,
         slot_count * SIZE_SLOT);

  uint32_t new_free_ptr = PAGE_SIZE;

  for (uint32_t i = 0; i < slot_count; ++i) {
    uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (i * SIZE_SLOT);
    uint32_t offset;
    std::memcpy(&offset, temp_page + slot_offset, sizeof(uint32_t));
    uint32_t size_field;
    std::memcpy(&size_field, temp_page + slot_offset + 4, sizeof(uint32_t));

    // If offset > 0 and size_field > 0, the tuple has physical bytes we must
    // preserve
    if (offset > 0 && size_field > 0) {
      // Strip the delete mask to get the actual physical size
      uint32_t actual_size = size_field & ~DELETE_MASK;
      new_free_ptr -= actual_size;

      memcpy(temp_page + new_free_ptr, GetData() + offset, actual_size);

      // Update the slot to point to the new compacted offset
      std::memcpy(temp_page + slot_offset, &new_free_ptr, sizeof(uint32_t));
    }
  }

  std::memcpy(temp_page + OFFSET_FREE_SPACE, &new_free_ptr, sizeof(uint32_t));
  memcpy(GetData(), temp_page, PAGE_SIZE);
}

bool TablePage::InsertTuple(const Tuple &tuple, RID *rid) {
  uint32_t tuple_size = tuple.GetSize();
  uint32_t slot_count = GetSlotCount();
  uint32_t target_slot = 0xFFFFFFFF; // Invalid slot sentinel

  // 1. Slot Reuse: Scan for a fully dead slot (offset == 0 && size == 0)
  for (uint32_t i = 0; i < slot_count; ++i) {
    uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (i * SIZE_SLOT);
    uint32_t offset;
    std::memcpy(&offset, GetData() + slot_offset, sizeof(uint32_t));
    uint32_t size;
    std::memcpy(&size, GetData() + slot_offset + 4, sizeof(uint32_t));
    if (offset == 0 && size == 0) {
      target_slot = i;
      break;
    }
  }

  bool requires_new_slot = (target_slot == 0xFFFFFFFF);
  uint32_t required_space = tuple_size + (requires_new_slot ? SIZE_SLOT : 0);

  // 2. Compaction Check
  if (GetFreeSpaceRemaining() < required_space) {
    Compact();
    // Verify if compaction actually freed enough contiguous space
    if (GetFreeSpaceRemaining() < required_space) {
      return false;
    }
  }

  // 3. Allocate Data
  uint32_t free_ptr = GetFreeSpacePointer();
  uint32_t new_free_ptr = free_ptr - tuple_size;
  memcpy(GetData() + new_free_ptr, tuple.GetData(), tuple_size);

  // 4. Update Metadata
  if (requires_new_slot) {
    target_slot = slot_count;
    SetSlotCount(slot_count + 1);
  }

  SetSlot(target_slot, new_free_ptr, tuple_size);
  SetFreeSpacePointer(new_free_ptr);

  rid->Set(GetPageId(), target_slot);
  return true;
}

bool TablePage::ForceInsertTuple(const Tuple &tuple, const RID &rid) {
  uint32_t slot_id = rid.GetSlotId();
  uint32_t tuple_size = tuple.GetSize();

  if (slot_id >= GetSlotCount()) {
    SetSlotCount(slot_id + 1);
  }

  if (GetFreeSpaceRemaining() < tuple_size) {
    Compact();
    if (GetFreeSpaceRemaining() < tuple_size) {
      return false;
    }
  }

  uint32_t free_space_ptr = GetFreeSpacePointer();
  free_space_ptr -= tuple_size;
  SetFreeSpacePointer(free_space_ptr);

  tuple.SerializeTo(GetData() + free_space_ptr);
  SetSlot(slot_id, free_space_ptr, tuple_size);

  return true;
}

bool TablePage::MarkDelete(const RID &rid) {
  uint32_t slot_idx = rid.GetSlotId();
  if (slot_idx >= GetSlotCount())
    return false;

  uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
  uint32_t tuple_size;
  std::memcpy(&tuple_size, GetData() + slot_offset + 4, sizeof(uint32_t));

  // Flip the highest bit to mark as deleted without destroying the size integer
  tuple_size |= DELETE_MASK;
  std::memcpy(GetData() + slot_offset + 4, &tuple_size, sizeof(uint32_t));

  return true;
}

bool TablePage::ApplyDelete(const RID &rid) {
  uint32_t slot_id = rid.GetSlotId();
  if (slot_id >= GetSlotCount())
    return false;

  // Fully erase offset and size. This explicitly triggers slot reuse.
  SetSlot(slot_id, 0, 0);
  return true;
}

bool TablePage::GetTuple(const RID &rid, Tuple *tuple) {
  uint32_t slot_idx = rid.GetSlotId();
  if (slot_idx >= GetSlotCount())
    return false;

  uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
  uint32_t tuple_offset;
  std::memcpy(&tuple_offset, GetData() + slot_offset, sizeof(uint32_t));
  uint32_t tuple_size;
  std::memcpy(&tuple_size, GetData() + slot_offset + 4, sizeof(uint32_t));

  if (tuple_size == 0 || (tuple_size & DELETE_MASK)) {
    return false;
  }

  tuple->DeserializeFrom(GetData() + tuple_offset, tuple_size);
  tuple->SetRid(rid);
  return true;
}

bool TablePage::UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple,
                            const RID &rid) {
  uint32_t slot_idx = rid.GetSlotId();
  if (slot_idx >= GetSlotCount())
    return false;

  uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
  uint32_t tuple_offset;
  std::memcpy(&tuple_offset, GetData() + slot_offset, sizeof(uint32_t));
  uint32_t tuple_size;
  std::memcpy(&tuple_size, GetData() + slot_offset + 4, sizeof(uint32_t));

  if (tuple_size == 0 || (tuple_size & DELETE_MASK)) {
    return false;
  }

  // Preserve old tuple for WAL
  if (old_tuple != nullptr) {
    old_tuple->DeserializeFrom(GetData() + tuple_offset, tuple_size);
    old_tuple->SetRid(rid);
  }

  uint32_t new_size = new_tuple.GetSize();

  // 1. In-place overwrite (perfect fit or shrinking)
  if (new_size <= tuple_size) {
    new_tuple.SerializeTo(GetData() + tuple_offset);
    if (new_size < tuple_size) {
      SetSlot(slot_idx, tuple_offset, new_size);
    }
    return true;
  }

  // 2. Out-of-place update on the same page (growing)
  // First, logically orphan the old bytes without triggering DELETE_MASK
  // We do this by zeroing them in the compaction logic implicitly:
  // since the offset changes, those byte areas become unreachable dead space.

  if (GetFreeSpaceRemaining() < new_size) {
    // Temporarily erase the old slot size so compaction claims its space
    SetSlot(slot_idx, 0, 0);
    Compact();

    // Check if compaction freed enough
    if (GetFreeSpaceRemaining() < new_size) {
      // Restore the slot pointer if we failed to find space
      SetSlot(slot_idx, tuple_offset, tuple_size);
      return false;
    }
  }

  // Allocate new contiguous space
  uint32_t free_ptr = GetFreeSpacePointer();
  uint32_t new_free_ptr = free_ptr - new_size;
  new_tuple.SerializeTo(GetData() + new_free_ptr);

  // Update Metadata
  SetSlot(slot_idx, new_free_ptr, new_size);
  SetFreeSpacePointer(new_free_ptr);

  return true;
}

bool TablePage::RollbackDelete(const RID &rid, const Tuple &tuple) {
  uint32_t slot_idx = rid.GetSlotId();
  if (slot_idx >= GetSlotCount())
    return false;

  uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
  uint32_t tuple_size;
  std::memcpy(&tuple_size, GetData() + slot_offset + 4, sizeof(uint32_t));

  // Strip the delete mask to restore the tuple to valid status
  tuple_size &= ~DELETE_MASK;
  std::memcpy(GetData() + slot_offset + 4, &tuple_size, sizeof(uint32_t));

  return true;
}

void TablePage::SetSlot(uint32_t slot_idx, uint32_t offset, uint32_t length) {
  uint32_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
  std::memcpy(GetData() + slot_offset, &offset, sizeof(uint32_t));
  std::memcpy(GetData() + slot_offset + 4, &length, sizeof(uint32_t));
}

uint32_t TablePage::GetFreeSpaceRemaining() {
  uint32_t free_ptr = GetFreeSpacePointer();
  uint32_t header_end = SIZE_TABLE_PAGE_HEADER + (GetSlotCount() * SIZE_SLOT);

  if (header_end > free_ptr)
    return 0;
  return free_ptr - header_end;
}

} // namespace tetodb