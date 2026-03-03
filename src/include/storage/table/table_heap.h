// table_heap.h

#pragma once

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/page/table_page.h"
#include "storage/table/tuple.h"
#include "storage/table/table_iterator.h"
#include "recovery/log_manager.h" 
#include <vector>
#include <unordered_map>
#include <mutex>
#include <algorithm>

namespace tetodb {

    class Transaction;

    // Custom Max-Heap for O(1) space checks and O(log N) updates
    class FreeSpaceHeap {
    private:
        struct Node {
            uint32_t free_space;
            page_id_t page_id;
        };

        std::vector<Node> heap_;
        std::unordered_map<page_id_t, size_t> indices_;

        void Swap(size_t i, size_t j) {
            std::swap(heap_[i], heap_[j]);
            indices_[heap_[i].page_id] = i;
            indices_[heap_[j].page_id] = j;
        }

        void SiftUp(size_t i) {
            while (i > 0) {
                size_t parent = (i - 1) / 2;
                if (heap_[i].free_space > heap_[parent].free_space) {
                    Swap(i, parent);
                    i = parent;
                }
                else break;
            }
        }

        void SiftDown(size_t i) {
            size_t size = heap_.size();
            while (true) {
                size_t left = 2 * i + 1;
                size_t right = 2 * i + 2;
                size_t largest = i;

                if (left < size && heap_[left].free_space > heap_[largest].free_space) largest = left;
                if (right < size && heap_[right].free_space > heap_[largest].free_space) largest = right;

                if (largest != i) {
                    Swap(i, largest);
                    i = largest;
                }
                else break;
            }
        }

    public:
        void Update(page_id_t page_id, uint32_t free_space) {
            if (indices_.find(page_id) == indices_.end()) {
                size_t idx = heap_.size();
                heap_.push_back({ free_space, page_id });
                indices_[page_id] = idx;
                SiftUp(idx);
            }
            else {
                size_t idx = indices_[page_id];
                uint32_t old_space = heap_[idx].free_space;
                heap_[idx].free_space = free_space;
                if (free_space > old_space) SiftUp(idx);
                else SiftDown(idx);
            }
        }

        page_id_t GetBestPage(uint32_t required_space) {
            if (heap_.empty() || heap_[0].free_space < required_space) return INVALID_PAGE_ID;
            return heap_[0].page_id;
        }

        void Clear() {
            heap_.clear();
            indices_.clear();
        }
    };

    class TableHeap {
    public:
        TableHeap(BufferPoolManager* bpm, LogManager* log_manager = nullptr, Transaction* txn = nullptr);

        TableHeap(BufferPoolManager* bpm, page_id_t first_page_id, LogManager* log_manager = nullptr);

        inline BufferPoolManager* GetBufferPoolManager() { return bpm_; }

        inline TableIterator Begin(Transaction* txn = nullptr) {
            RID first_rid(first_page_id_, 0);
            return TableIterator(this, first_rid);
        }

        inline TableIterator End() {
            RID end_rid(INVALID_PAGE_ID, 0);
            return TableIterator(this, end_rid);
        }

        inline page_id_t GetFirstPageId() { return first_page_id_; }

        bool InsertTuple(const Tuple& tuple, RID* rid, Transaction* txn = nullptr);

        bool GetTuple(const RID& rid, Tuple* tuple, Transaction* txn = nullptr);

        bool MarkDelete(const RID& rid, Transaction* txn = nullptr);

        bool UpdateTuple(const Tuple& tuple, RID* rid, Transaction* txn = nullptr);

        bool RollbackDelete(const RID& rid, const Tuple& tuple, Transaction* txn = nullptr);

        void Destroy();

    private:
        BufferPoolManager* bpm_;
        LogManager* log_manager_;
        page_id_t first_page_id_;
        page_id_t last_page_id_;

        FreeSpaceHeap fsm_;
        std::mutex latch_;
    };

} // namespace tetodb