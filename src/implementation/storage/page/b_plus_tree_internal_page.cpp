#include <algorithm> // for std::copy, std::move_backward
#include <iostream>
#include <sstream>

#include "index/generic_key.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include <new>

namespace tetodb {

/*****************************************************************************
 * LOOKUP (Binary Search - Optimized)
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  // Range: [1, size) because index 0 is invalid key
  uint32_t left = 1;
  uint32_t right = GetSize() - 1;
  uint32_t target = 0;

  while (left <= right) {
    uint32_t mid = left + (right - left) / 2;
    if (comparator(array_[mid].first, key) <= 0) {
      target = mid;
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return array_[target].second;
}

/*****************************************************************************
 * POPULATE NEW ROOT
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  SetSize(2);
  array_[0].second = old_value; // Left Child
  array_[1].first = new_key;    // Separator Key
  array_[1].second = new_value; // Right Child
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  uint32_t total_size = GetSize();
  uint32_t split_idx = total_size / 2;
  uint32_t move_count = total_size - split_idx;

  // OPTIMIZATION: Use std::copy (compiles to memcpy)
  std::copy(array_ + split_idx, array_ + total_size, recipient->array_);

  recipient->SetSize(move_count);
  SetSize(split_idx);

  // CRITICAL: We moved children to a new parent. Update their Parent ID.
  // This part is I/O heavy but unavoidable.
  for (uint32_t i = 0; i < move_count; i++) {
    page_id_t child_page_id = recipient->ValueAt(i);
    Page *child_page = buffer_pool_manager->FetchPage(child_page_id);

    if (child_page != nullptr) {
      auto *b_node = new (child_page->GetData()) BPlusTreePage;
      b_node->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(child_page_id, true);
    }
  }
}

/*****************************************************************************
 * INSERT NODE AFTER (The Helper)
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  // 1. Find the index of the old_value
  // Linear scan is fine here (Size ~ 200).
  // Optimization: Buffer Pool pinning dominates cost, not this loop.
  uint32_t index = GetSize(); // Default to invalid
  for (uint32_t i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == old_value) {
      index = i;
      break;
    }
  }

  // Safety check
  if (index == GetSize()) {
    return; // Should not happen
  }

  // 2. Shift Elements (OPTIMIZED)
  // std::move_backward is memmove. Much faster than loop.
  // Shift [index+1, end) -> [index+2, end+1)
  std::move_backward(array_ + index + 1, array_ + GetSize(),
                     array_ + GetSize() + 1);

  // 3. Insert
  array_[index + 1].first = new_key;
  array_[index + 1].second = new_value;

  IncreaseSize(1);
}

// Explicit Instantiation
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;

} // namespace tetodb