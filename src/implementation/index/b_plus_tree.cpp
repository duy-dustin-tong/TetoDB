// b_plus_tree.cpp

#include <iostream>
#include <string>

#include "common/exceptions.h"
#include "common/record_id.h"
#include "index/b_plus_tree.h"
#include "index/generic_key.h"
#include "storage/page/header_page.h"
#include <cstring>

namespace tetodb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          uint32_t leaf_max_size, uint32_t internal_max_size,
                          page_id_t root_page_id)
    : index_name_(std::move(name)), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator),
      leaf_max_size_(leaf_max_size), internal_max_size_(internal_max_size) {}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> *result,
                              Transaction *transaction) {
  try {
    if (IsEmpty())
      return false;

    Page *page = FindLeafPage(key);
    if (page == nullptr)
      return false;

    auto *leaf_page = reinterpret_cast<
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
        page->GetData());

    bool found = false;

    // 1. Find the starting index of the key
    uint32_t idx = leaf_page->KeyIndex(key, comparator_);

    // 2. Loop forward and collect ALL duplicates
    while (true) {
      while (idx < leaf_page->GetSize() &&
             comparator_(leaf_page->KeyAt(idx), key) == 0) {
        result->push_back(leaf_page->ValueAt(idx));
        found = true;
        idx++;
      }

      // If we hit the end of this page, duplicates might continue on the next
      // page
      if (idx == leaf_page->GetSize() &&
          leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
        page_id_t next_page_id = leaf_page->GetNextPageId();

        Page *next_page = buffer_pool_manager_->FetchPage(next_page_id);
        next_page->RLatch();
        page->RUnlatch();
        buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

        page = next_page;
        leaf_page = reinterpret_cast<
            BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
            page->GetData());
        idx = 0; // Reset index for the new page
      } else {
        break; // No more duplicates, or end of the tree
      }
    }

    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);

    return found;
  } catch (Exception &e) {
    UnlockUnpinPages(transaction);
    throw;
  }
}
/*****************************************************************************
 * UTILITIES AND HELPERS
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost,
                                   Operation op, Transaction *transaction) {

  if (IsEmpty())
    return nullptr;

  bool root_locked = false;
  if (op == Operation::READ) {
    root_latch_.RLock();
  } else {
    root_latch_.WLock();
    root_locked = true;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    if (op == Operation::READ)
      root_latch_.RUnlock();
    else
      root_latch_.WUnlock();
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }

  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (op == Operation::READ) {
    page->RLatch();
    root_latch_.RUnlock();
  } else {
    page->WLatch();
    if (IsSafe(node, op)) {
      root_latch_.WUnlock();
      root_locked = false;
    } else {
      if (transaction != nullptr) {
        transaction->AddLockedLatch(&root_latch_);
      }
    }
  }

  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        page->GetData());

    page_id_t child_page_id;
    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, comparator_);
    }

    Page *child_page = buffer_pool_manager_->FetchPage(child_page_id);
    if (child_page == nullptr) {
      if (op == Operation::READ)
        page->RUnlatch();
      else {
        page->WUnlatch();
        UnlockUnpinPages(transaction);
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
    }

    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (op == Operation::READ) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();

      if (IsSafe(child_node, op)) {
        UnlockUnpinPages(transaction);
        if (root_locked) {
          root_locked = false;
        }
      }

      // --- FIX: Prevent Nullptr Dereference during PopulateIndex ---
      if (transaction != nullptr) {
        transaction->AddIntoPageSet(page);
      } else {
        // Safe fallback for single-threaded boot operations to prevent pin
        // leaks
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
      }
    }

    page = child_page;
    node = child_node;
  }

  return page;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  try {
    if (transaction != nullptr) {
      uint32_t target_size = depth_ + 2;
      if (transaction->GetPageSet()->capacity() < target_size) {
        transaction->GetPageSet()->reserve(target_size);
      }
    }

    root_latch_.WLock();
    if (IsEmpty()) {
      StartNewTree(key, value);
      root_latch_.WUnlock();
      return true;
    }
    root_latch_.WUnlock();

    return InsertIntoLeaf(key, value, transaction);

  } catch (Exception &e) {
    UnlockUnpinPages(transaction);
    throw;
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Remove(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  try {
    if (transaction != nullptr) {
      uint32_t target_size = depth_ + 2;
      if (transaction->GetPageSet()->capacity() < target_size) {
        transaction->GetPageSet()->reserve(target_size);
      }
    }

    if (IsEmpty()) {
      return false;
    }

    return RemoveFromLeaf(key, value, transaction);
  } catch (Exception &e) {
    UnlockUnpinPages(transaction);
    throw;
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }

  auto *leaf =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          page->GetData());
  leaf->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf->Insert(key, value, comparator_);

  root_page_id_ = page_id;
  depth_ = 1;

  buffer_pool_manager_->UnpinPage(page_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  Page *page = FindLeafPage(key, false, Operation::INSERT, transaction);
  if (page == nullptr)
    return false;

  auto *leaf =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          page->GetData());

  try {
    uint32_t size = leaf->GetSize();
    uint32_t new_size = leaf->Insert(key, value, comparator_);

    if (new_size == size) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      UnlockUnpinPages(transaction);
      return false;
    }

    if (new_size > leaf_max_size_) {
      BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *new_leaf =
          Split(leaf);

      try {
        InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);
      } catch (Exception &e) {
        buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), false);
        throw;
      }

      buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
    }
  } catch (Exception &e) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    throw;
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  UnlockUnpinPages(transaction);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::RemoveFromLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  Page *page = FindLeafPage(key, false, Operation::DELETE, transaction);
  if (page == nullptr)
    return false;

  auto *leaf =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          page->GetData());

  try {
    uint32_t size = leaf->GetSize();
    uint32_t new_size = leaf->Remove(key, value, comparator_);

    if (new_size == size) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), false);
      UnlockUnpinPages(transaction);
      return false;
    }

  } catch (Exception &e) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
    throw;
  }

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  UnlockUnpinPages(transaction);

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");
  }

  auto *new_node = reinterpret_cast<N *>(page->GetData());
  new_node->Init(page_id, node->GetParentPageId(), node->GetMaxSize());
  node->MoveHalfTo(new_node, buffer_pool_manager_);

  if (node->IsLeafPage()) {
    auto *leaf = reinterpret_cast<
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(node);
    auto *new_leaf = reinterpret_cast<
        BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(new_node);

    new_leaf->SetNextPageId(leaf->GetNextPageId());
    leaf->SetNextPageId(new_leaf->GetPageId());
  }

  return new_node;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    page_id_t new_root_id;
    Page *page = buffer_pool_manager_->NewPage(&new_root_id);
    if (page == nullptr)
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");

    auto *new_root = reinterpret_cast<
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        page->GetData());
    new_root->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key,
                              new_node->GetPageId());

    old_node->SetParentPageId(new_root_id);
    new_node->SetParentPageId(new_root_id);

    root_page_id_ = new_root_id;
    depth_++;
    buffer_pool_manager_->UnpinPage(new_root_id, true);
    return;
  }

  page_id_t parent_id = old_node->GetParentPageId();
  Page *page = buffer_pool_manager_->FetchPage(parent_id);
  if (page == nullptr)
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Out of memory");

  auto *parent = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      page->GetData());

  try {
    parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

    if (parent->GetSize() > internal_max_size_) {
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *new_parent =
          Split(parent);

      try {
        InsertIntoParent(parent, new_parent->KeyAt(0), new_parent, transaction);
      } catch (Exception &e) {
        buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), false);
        throw;
      }

      buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
    }
  } catch (Exception &e) {
    buffer_pool_manager_->UnpinPage(parent_id, true);
    throw;
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    std::cout << "Tree is Empty." << std::endl;
    return;
  }

  std::queue<std::pair<page_id_t, int>> q;
  q.push({root_page_id_, 0});

  int current_depth = 0;

  while (!q.empty()) {
    auto [pid, depth] = q.front();
    q.pop();

    if (depth > current_depth) {
      std::cout << "\n";
      current_depth = depth;
    }

    Page *page = bpm->FetchPage(pid);
    auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    std::cout << "[PID=" << pid << " Type=" << (node->IsLeafPage() ? "L" : "I")
              << " Size=" << node->GetSize() << "] ";

    if (!node->IsLeafPage()) {
      auto *internal = reinterpret_cast<
          BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
          page->GetData());
      for (int i = 0; i < internal->GetSize(); i++) {
        q.push({internal->ValueAt(i), depth + 1});
      }
    }

    bpm->UnpinPage(pid, false);
  }
  std::cout << "\n";
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  if (IsEmpty())
    return End();

  Page *page = FindLeafPage(KeyType(), true);
  if (page == nullptr)
    return End();

  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, 0);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if (IsEmpty())
    return End();

  Page *page = FindLeafPage(key, false);
  if (page == nullptr)
    return End();

  auto *leaf =
      reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *>(
          page->GetData());
  uint32_t index = leaf->KeyIndex(key, comparator_);

  return INDEXITERATOR_TYPE(buffer_pool_manager_, page, index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  if (transaction == nullptr)
    return;

  for (Page *page : *transaction->GetPageSet()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  transaction->GetPageSet()->clear();

  for (ReaderWriterLatch *latch : *transaction->GetLockedLatches()) {
    latch->WUnlock();
  }
  transaction->GetLockedLatches()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N> bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {
  if (op == Operation::INSERT) {
    return node->GetSize() < node->GetMaxSize();
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
  if (IsEmpty())
    return;

  root_latch_.WLock();
  DestroyNode(root_page_id_);
  root_page_id_ = INVALID_PAGE_ID;
  depth_ = 0;
  root_latch_.WUnlock();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DestroyNode(page_id_t page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr)
    return;

  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(node);
    for (int i = 0; i < internal->GetSize(); i++) {
      DestroyNode(internal->ValueAt(i));
    }
  }

  buffer_pool_manager_->UnpinPage(page_id, false);
  buffer_pool_manager_->DeletePage(page_id);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;
template class BPlusTree<GenericKey<128>, RID, GenericComparator<128>>;
template class BPlusTree<GenericKey<256>, RID, GenericComparator<256>>;

} // namespace tetodb