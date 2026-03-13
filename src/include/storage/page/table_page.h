// table_page.h

#pragma once

#include "common/config.h"
#include "common/record_id.h"
#include "storage/page/page.h"
#include "storage/table/tuple.h"
#include <cstring>

namespace tetodb {

static constexpr size_t OFFSET_PAGE_ID = 0;
static constexpr size_t OFFSET_LSN = 4;
static constexpr size_t OFFSET_PREV_PAGE_ID = 8;
static constexpr size_t OFFSET_NEXT_PAGE_ID = 12;
static constexpr size_t OFFSET_FREE_SPACE = 16;
static constexpr size_t OFFSET_SLOT_COUNT = 20;
static constexpr size_t SIZE_TABLE_PAGE_HEADER = 24;
static constexpr size_t SIZE_SLOT = 8;

// Bitmask to flag a tuple as logically deleted without destroying its size
// metadata
static constexpr uint32_t DELETE_MASK = 0x80000000;

class TablePage : public Page {
public:
  void Init(page_id_t page_id, uint32_t page_size = PAGE_SIZE,
            page_id_t prev_page_id = INVALID_PAGE_ID, lsn_t lsn = INVALID_LSN);

  bool InsertTuple(const Tuple &tuple, RID *rid);
  bool ForceInsertTuple(const Tuple &tuple, const RID &rid);
  bool MarkDelete(const RID &rid);
  bool ApplyDelete(const RID &rid);
  bool GetTuple(const RID &rid, Tuple *tuple);
  bool UpdateTuple(const Tuple &new_tuple, Tuple *old_tuple, const RID &rid);
  bool RollbackDelete(const RID &rid, const Tuple &tuple);

  // Physically defragments the page to reclaim dead space
  void Compact();

  inline page_id_t GetPageId() {
    page_id_t val;
    std::memcpy(&val, GetData() + OFFSET_PAGE_ID, sizeof(page_id_t));
    return val;
  }
  inline void SetPageId(page_id_t page_id) {
    std::memcpy(GetData() + OFFSET_PAGE_ID, &page_id, sizeof(page_id_t));
  }

  inline lsn_t GetLSN() {
    lsn_t val;
    std::memcpy(&val, GetData() + OFFSET_LSN, sizeof(lsn_t));
    return val;
  }
  inline void SetLSN(lsn_t lsn) {
    std::memcpy(GetData() + OFFSET_LSN, &lsn, sizeof(lsn_t));
  }

  inline page_id_t GetPrevPageId() {
    page_id_t val;
    std::memcpy(&val, GetData() + OFFSET_PREV_PAGE_ID, sizeof(page_id_t));
    return val;
  }
  inline void SetPrevPageId(page_id_t prev_page_id) {
    std::memcpy(GetData() + OFFSET_PREV_PAGE_ID, &prev_page_id,
                sizeof(page_id_t));
  }

  inline page_id_t GetNextPageId() {
    page_id_t val;
    std::memcpy(&val, GetData() + OFFSET_NEXT_PAGE_ID, sizeof(page_id_t));
    return val;
  }
  inline void SetNextPageId(page_id_t next_page_id) {
    std::memcpy(GetData() + OFFSET_NEXT_PAGE_ID, &next_page_id,
                sizeof(page_id_t));
  }

  inline uint32_t GetFreeSpacePointer() {
    uint32_t val;
    std::memcpy(&val, GetData() + OFFSET_FREE_SPACE, sizeof(uint32_t));
    return val;
  }
  inline void SetFreeSpacePointer(uint32_t ptr) {
    std::memcpy(GetData() + OFFSET_FREE_SPACE, &ptr, sizeof(uint32_t));
  }

  inline uint32_t GetSlotCount() {
    uint32_t val;
    std::memcpy(&val, GetData() + OFFSET_SLOT_COUNT, sizeof(uint32_t));
    return val;
  }
  inline void SetSlotCount(uint32_t count) {
    std::memcpy(GetData() + OFFSET_SLOT_COUNT, &count, sizeof(uint32_t));
  }

  inline bool IsValidTuple(uint32_t slot_idx) {
    if (slot_idx >= GetSlotCount())
      return false;
    size_t slot_offset = SIZE_TABLE_PAGE_HEADER + (slot_idx * SIZE_SLOT);
    uint32_t tuple_size;
    std::memcpy(&tuple_size, GetData() + slot_offset + 4, sizeof(uint32_t));

    // Valid if size > 0 AND the delete mask is not set
    return tuple_size > 0 && (tuple_size & DELETE_MASK) == 0;
  }

  uint32_t GetFreeSpaceRemaining();

private:
  void SetSlot(uint32_t slot_idx, uint32_t offset, uint32_t length);
};

} // namespace tetodb