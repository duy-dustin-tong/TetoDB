// b_plus_tree_page.h

#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <cstdint>
#include <string>

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/table/tuple.h"

namespace tetodb {

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

    // define page type enum
    enum class IndexPageType { INVALID_PAGE_ID = 0, LEAF_PAGE, INTERNAL_PAGE };

    /**
     * Both Internal and Leaf pages inherit from this.
     *
     * Header Format (size in byte, 24 bytes total):
     * ----------------------------------------------------------------------------
     * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) | PageId (4) |
     * ----------------------------------------------------------------------------
     */
    class BPlusTreePage {
    public:
        // --- Metadata Getters/Setters (Implicitly Inline) ---

        inline bool IsLeafPage() const { return page_type_ == IndexPageType::LEAF_PAGE; }
        inline bool IsRootPage() const { return parent_page_id_ == INVALID_PAGE_ID; }
        inline void SetPageType(IndexPageType page_type) { page_type_ = page_type; }

        inline uint32_t GetSize() const { return size_; }
        inline void SetSize(uint32_t size) { size_ = size; }
        inline void IncreaseSize(uint32_t amount) { size_ += amount; }

        inline uint32_t GetMaxSize() const { return max_size_; }
        inline void SetMaxSize(uint32_t max_size) { max_size_ = max_size; }

        inline uint32_t GetMinSize() const {
            if (IsRootPage()) {
                return IsLeafPage() ? 1 : 2;
            }
            return max_size_ / 2;
        }

        inline page_id_t GetParentPageId() const { return parent_page_id_; }
        inline void SetParentPageId(page_id_t parent_page_id) { parent_page_id_ = parent_page_id; }

        inline page_id_t GetPageId() const { return page_id_; }
        inline void SetPageId(page_id_t page_id) { page_id_ = page_id; }

        inline void SetLSN(lsn_t lsn = INVALID_LSN) { lsn_ = lsn; }

    private:
        // Member variables
        IndexPageType page_type_ [[maybe_unused]];
        lsn_t lsn_ [[maybe_unused]];
        uint32_t size_ [[maybe_unused]];
        uint32_t max_size_ [[maybe_unused]];
        page_id_t parent_page_id_ [[maybe_unused]];
        page_id_t page_id_ [[maybe_unused]];
    };

}  // namespace tetodb