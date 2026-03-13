// b_plus_tree.h

#pragma once

#include <queue>
#include <string>
#include <vector>

#include "index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "concurrency/transaction.h"

namespace tetodb {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

    // 1. Define Operation Modes
    enum class Operation { READ, INSERT, DELETE };

    INDEX_TEMPLATE_ARGUMENTS
    class BPlusTree {
    public:
        // --- UPDATED CONSTRUCTOR ---
        explicit BPlusTree(std::string name, BufferPoolManager* buffer_pool_manager, const KeyComparator& comparator,
            uint32_t leaf_max_size = LEAF_PAGE_SIZE, uint32_t internal_max_size = INTERNAL_PAGE_SIZE,
            page_id_t root_page_id = INVALID_PAGE_ID);

        inline bool IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
        inline uint32_t GetDepth() { return depth_; }
        inline page_id_t GetRootPageId() const { return root_page_id_; } // <-- NEW

        // Core Public Methods
        bool Insert(const KeyType& key, const ValueType& value, Transaction* transaction = nullptr);
        bool Remove(const KeyType& key, const ValueType& value, Transaction* transaction = nullptr);
        bool GetValue(const KeyType& key, std::vector<ValueType>* result, Transaction* transaction = nullptr);

        // Iterators
        INDEXITERATOR_TYPE Begin();
        INDEXITERATOR_TYPE Begin(const KeyType& key);
        INDEXITERATOR_TYPE End();

        void Print(BufferPoolManager* bpm);
        void Destroy();

    private:
        // --- HELPER FUNCTIONS ---

        void StartNewTree(const KeyType& key, const ValueType& value);

        // Modified: Now accepts Transaction pointer
        bool InsertIntoLeaf(const KeyType& key, const ValueType& value, Transaction* transaction = nullptr);

        // NEW: Helper to handle deletion logic (and future merging/redistribution)
        bool RemoveFromLeaf(const KeyType& key, const ValueType& value, Transaction* transaction = nullptr);

        void DestroyNode(page_id_t page_id);

        template <typename N>
        N* Split(N* node);

        void InsertIntoParent(BPlusTreePage* old_node, const KeyType& key, BPlusTreePage* new_node,
            Transaction* transaction = nullptr);

        // Modified: Now accepts Operation Mode and Transaction
        // This is the heart of Latch Crabbing.
        Page* FindLeafPage(const KeyType& key, bool leftMost = false,
            Operation op = Operation::READ, Transaction* transaction = nullptr);


        // --- CONCURRENCY HELPERS ---

        // Unlatch and Unpin all pages in the transaction's page_set_
        void UnlockUnpinPages(Transaction* transaction);

        // Check if a node is "Safe" (won't split/merge)
        template <typename N>
        bool IsSafe(N* node, Operation op);


        // Members
        std::string index_name_;
        page_id_t root_page_id_;
        BufferPoolManager* buffer_pool_manager_;
        KeyComparator comparator_;
        uint32_t leaf_max_size_;
        uint32_t internal_max_size_;


        std::atomic<uint32_t> depth_ = 0;

        // Global latch to protect the root_page_id_ variable itself during tree creation
        ReaderWriterLatch root_latch_;
    };

}  // namespace tetodb