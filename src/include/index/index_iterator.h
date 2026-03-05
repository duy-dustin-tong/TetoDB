// index_iterator.h

#pragma once

#include "storage/page/b_plus_tree_leaf_page.h"
#include <new>

namespace tetodb {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // Constructor
  // The page passed in here is already PINNED and READ-LATCHED by the caller
  // (BPlusTree::Begin)
  IndexIterator(BufferPoolManager *bpm, Page *page, uint32_t index)
      : buffer_pool_manager_(bpm), page_(page), index_(index) {
    if (page_ != nullptr) {
      // Safe cast: reinterpret_cast preserves existing page data
      leaf_ = reinterpret_cast<
          BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          page_->GetData());

      // If we are initialized pointing at the end of the page,
      // we must jump to the next page immediately to avoid reading garbage.
      if (index_ >= leaf_->GetSize()) {
        Advance();
      }
    } else {
      leaf_ = nullptr;
      index_ = 0;
    }
  }

  // Move constructor — transfer ownership of the latched page
  IndexIterator(IndexIterator &&other) noexcept
      : buffer_pool_manager_(other.buffer_pool_manager_), page_(other.page_),
        leaf_(other.leaf_), index_(other.index_) {
    other.page_ = nullptr;
    other.leaf_ = nullptr;
    other.index_ = 0;
  }

  // Move assignment
  IndexIterator &operator=(IndexIterator &&other) noexcept {
    if (this != &other) {
      // Release our current page first
      if (page_ != nullptr) {
        page_->RUnlatch();
        buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
      }
      buffer_pool_manager_ = other.buffer_pool_manager_;
      page_ = other.page_;
      leaf_ = other.leaf_;
      index_ = other.index_;
      other.page_ = nullptr;
      other.leaf_ = nullptr;
      other.index_ = 0;
    }
    return *this;
  }

  // Delete copy operations to prevent double-free
  IndexIterator(const IndexIterator &) = delete;
  IndexIterator &operator=(const IndexIterator &) = delete;

  // Destructor
  ~IndexIterator() {
    if (page_ != nullptr) {
      page_->RUnlatch(); // Unlock the read lock
      buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);
    }
  }

  inline bool IsEnd() const { return leaf_ == nullptr; }

  // --- HOT PATH: DEREFERENCE ---
  inline const MappingType &operator*() const { return leaf_->ItemAt(index_); }

  // --- HOT PATH: INCREMENT ---
  inline IndexIterator &operator++() {
    if (IsEnd())
      return *this;

    index_++;

    // Check if we need to jump to the next page
    if (index_ >= leaf_->GetSize()) {
      Advance();
    }
    return *this;
  }

  inline bool operator==(const IndexIterator &itr) const {
    if (leaf_ == nullptr && itr.leaf_ == nullptr)
      return true;
    if (leaf_ == nullptr || itr.leaf_ == nullptr)
      return false;
    return leaf_->GetPageId() == itr.leaf_->GetPageId() && index_ == itr.index_;
  }

  inline bool operator!=(const IndexIterator &itr) const {
    return !(*this == itr);
  }

private:
  inline void Advance() {
    page_id_t next_page_id = leaf_->GetNextPageId();

    page_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_->GetPageId(), false);

    if (next_page_id == INVALID_PAGE_ID) {
      page_ = nullptr;
      leaf_ = nullptr;
      index_ = 0;
    } else {
      page_ = buffer_pool_manager_->FetchPage(next_page_id);
      if (page_ != nullptr) {
        page_->RLatch();
        leaf_ = reinterpret_cast<
            BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
            page_->GetData());
        index_ = 0;
      } else {
        leaf_ = nullptr;
      }
    }
  }

private:
  BufferPoolManager *buffer_pool_manager_;
  Page *page_ = nullptr;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_ = nullptr;
  uint32_t index_ = 0;
};

} // namespace tetodb