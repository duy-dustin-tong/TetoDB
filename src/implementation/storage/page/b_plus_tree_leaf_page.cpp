// b_plus_tree_leaf_page.cpp
#include <algorithm>
#include <sstream>

#include "common/record_id.h"
#include "index/generic_key.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace tetodb {

INDEX_TEMPLATE_ARGUMENTS
uint32_t
B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key,
                                     const KeyComparator &comparator) const {
  uint32_t left = 0;
  uint32_t right = GetSize();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    if (comparator(array_[mid].first, key) < 0)
      left = mid + 1;
    else
      right = mid;
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
uint32_t B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                            const ValueType &value,
                                            const KeyComparator &comparator) {
  uint32_t idx = KeyIndex(key, comparator);
  uint32_t current_size = GetSize();

  // --- NO DUPLICATE CHECK ---
  // We now natively allow identical keys to sit next to each other in memory.

  std::move_backward(array_ + idx, array_ + current_size,
                     array_ + current_size + 1);
  array_[idx] = {key, value};
  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value,
                                        const KeyComparator &comparator) const {
  uint32_t idx = KeyIndex(key, comparator);
  if (idx < GetSize() && comparator(array_[idx].first, key) == 0) {
    *value = array_[idx].second;
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
uint32_t B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key,
                                            const ValueType &value,
                                            const KeyComparator &comparator) {
  uint32_t target_idx = KeyIndex(key, comparator);
  uint32_t current_size = GetSize();

  // --- DUPLICATE AWARE REMOVAL ---
  // Loop through all duplicates of this key to find the exact matching RID
  for (uint32_t i = target_idx; i < current_size; i++) {
    if (comparator(array_[i].first, key) == 0) {
      if (array_[i].second == value) { // Found the exact row!
        std::move(array_ + i + 1, array_ + current_size, array_ + i);
        IncreaseSize(-1);
        break;
      }
    } else {
      break; // Passed the duplicates, stop searching
    }
  }
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) {
  uint32_t total_size = GetSize();
  uint32_t split_idx = total_size / 2;
  uint32_t move_count = total_size - split_idx;

  std::copy(array_ + split_idx, array_ + total_size, recipient->array_);
  recipient->SetSize(move_count);
  SetSize(split_idx);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  uint32_t my_size = GetSize();
  uint32_t target_start_idx = recipient->GetSize();

  std::copy(array_, array_ + my_size, recipient->array_ + target_start_idx);
  recipient->IncreaseSize(my_size);
  SetSize(0);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
template class BPlusTreeLeafPage<GenericKey<128>, RID, GenericComparator<128>>;
template class BPlusTreeLeafPage<GenericKey<256>, RID, GenericComparator<256>>;

} // namespace tetodb