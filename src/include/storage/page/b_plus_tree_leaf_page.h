// b_plus_tree_leaf_page.h
#pragma once

#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace tetodb {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

    INDEX_TEMPLATE_ARGUMENTS
        class BPlusTreeLeafPage : public BPlusTreePage {
        public:
            inline void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, uint32_t max_size = LEAF_PAGE_SIZE) {
                SetPageType(IndexPageType::LEAF_PAGE);
                SetSize(0);
                SetPageId(page_id);
                SetParentPageId(parent_id);
                SetNextPageId(INVALID_PAGE_ID);
                SetMaxSize(max_size);
            }

            inline page_id_t GetNextPageId() const { return next_page_id_; }
            inline void SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
            inline KeyType KeyAt(uint32_t index) const { return array_[index].first; }
            inline ValueType ValueAt(uint32_t index) const { return array_[index].second; }
            inline const MappingType& ItemAt(uint32_t index) const { return array_[index]; }

            uint32_t KeyIndex(const KeyType& key, const KeyComparator& comparator) const;
            bool Lookup(const KeyType& key, ValueType* value, const KeyComparator& comparator) const;

            uint32_t Insert(const KeyType& key, const ValueType& value, const KeyComparator& comparator);

            // --- UPDATED: Now requires ValueType (RID) to safely delete duplicates ---
            uint32_t Remove(const KeyType& key, const ValueType& value, const KeyComparator& comparator);

            void MoveHalfTo(BPlusTreeLeafPage* recipient, BufferPoolManager* buffer_pool_manager);
            void MoveAllTo(BPlusTreeLeafPage* recipient);

        private:
            page_id_t next_page_id_;
            MappingType array_[1];
    };

}  // namespace tetodb