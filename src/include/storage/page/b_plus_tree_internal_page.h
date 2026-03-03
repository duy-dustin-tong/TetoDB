// b_plus_tree_internal_page.h

#pragma once

#include <queue>
#include "storage/page/b_plus_tree_page.h"

namespace tetodb {

	// Internal Page maps Keys to PageIDs (Traffic Directors)
#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / sizeof(MappingType))

	INDEX_TEMPLATE_ARGUMENTS
	class BPlusTreeInternalPage : public BPlusTreePage {
	public:
		// Initialize the page
		inline void Init(page_id_t page_id, page_id_t parent_id, uint32_t max_size) {
			SetPageType(IndexPageType::INTERNAL_PAGE);
			SetSize(0);
			SetPageId(page_id);
			SetParentPageId(parent_id);
			SetMaxSize(max_size);
		}

		// Get key/value at index
		inline KeyType KeyAt(uint32_t index) const { return array_[index].first; }

		inline void SetKeyAt(uint32_t index, const KeyType& key) { array_[index].first = key; }

		inline ValueType ValueAt(uint32_t index) const { return array_[index].second; }

		// --- SEARCH LOGIC ---
		// Returns the PageID of the child that handles the given key.
		ValueType Lookup(const KeyType& key, const KeyComparator& comparator) const;

		// --- POPULATE NEW ROOT ---
		void PopulateNewRoot(const ValueType& old_value, const KeyType& new_key, const ValueType& new_value);

		// --- SPLIT & MERGE UTILS ---
		// Move half of keys to recipient (during Split)
		void MoveHalfTo(BPlusTreeInternalPage* recipient, BufferPoolManager* buffer_pool_manager);

		// Move all keys to recipient (during Merge)
		void MoveAllTo(BPlusTreeInternalPage* recipient, const KeyType& middle_key, BufferPoolManager* buffer_pool_manager);


		void InsertNodeAfter(const ValueType& old_value, const KeyType& new_key, const ValueType& new_value);
	private:
		// Flexible Array Member
		// MappingType is pair<KeyType, page_id_t>
		MappingType array_[1];
	};

}  // namespace tetodb