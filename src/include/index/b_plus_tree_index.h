// b_plus_tree_index.h

#pragma once

#include "index/abstract_index_iterator.h"
#include "index/b_plus_tree.h"
#include "index/generic_key.h"
#include "index/index.h"
#include <memory>


namespace tetodb {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndexIteratorWrapper : public AbstractIndexIterator {
public:
  // We take ownership of a live B+Tree Iterator and the search conditions
  BPlusTreeIndexIteratorWrapper(INDEXITERATOR_TYPE iter, KeyType search_key,
                                KeyComparator comparator)
      : iter_(std::move(iter)), search_key_(search_key),
        comparator_(comparator) {}

  bool IsEnd() const override { return iter_.IsEnd(); }

  void Advance() override { ++iter_; }

  RID GetCurrentRid() const override { return (*iter_).second; }

  // Because the B+Tree is sorted ascending, if our current key is strictly >
  // our target, we are out of bounds.
  bool IsPastSearchBound() const override {
    if (iter_.IsEnd())
      return true;
    return comparator_((*iter_).first, search_key_) > 0;
  }

private:
  INDEXITERATOR_TYPE iter_;
  KeyType search_key_;
  KeyComparator comparator_;
};

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex : public Index {
public:
  BPlusTreeIndex(std::string name, BufferPoolManager *bpm,
                 const KeyComparator &comparator, uint32_t leaf_max,
                 uint32_t internal_max, std::unique_ptr<Schema> key_schema,
                 page_id_t root_page_id = INVALID_PAGE_ID)
      : name_(std::move(name)), key_schema_(std::move(key_schema)) {
    b_tree_ = std::make_unique<BPlusTree<KeyType, ValueType, KeyComparator>>(
        name_, bpm, comparator, leaf_max, internal_max, root_page_id);
  }

  void InsertEntry(const Tuple &key_tuple, RID rid, Transaction *txn) override {
    KeyType index_key;
    // Extract the Value from our isolated key tuple and use your existing
    // method!
    index_key.SetFromValue(key_tuple.GetValue(key_schema_.get(), 0));
    b_tree_->Insert(index_key, rid, txn);
  }

  void DeleteEntry(const Tuple &key_tuple, RID rid, Transaction *txn) override {
    KeyType index_key;
    index_key.SetFromValue(key_tuple.GetValue(key_schema_.get(), 0));
    b_tree_->Remove(index_key, rid, txn);
  }

  void ScanKey(const Tuple &key_tuple, std::vector<RID> *result,
               Transaction *txn) override {
    KeyType index_key;
    index_key.SetFromValue(key_tuple.GetValue(key_schema_.get(), 0));
    b_tree_->GetValue(index_key, result, txn);
  }

  std::unique_ptr<AbstractIndexIterator>
  GetBeginIterator(const Tuple &key_tuple) override {
    KeyType index_key;
    index_key.SetFromValue(key_tuple.GetValue(key_schema_.get(), 0));

    // Create the raw B+Tree iterator
    auto raw_iter = b_tree_->Begin(index_key);

    // Package it into the type-erased wrapper and return
    return std::make_unique<
        BPlusTreeIndexIteratorWrapper<KeyType, ValueType, KeyComparator>>(
        std::move(raw_iter), index_key,
        KeyComparator(key_schema_->GetColumn(0).GetTypeId()));
  }

  // Add to public section of BPlusTreeIndex:
  void Destroy() override { b_tree_->Destroy(); }

  std::string GetName() const override { return name_; }
  const Schema *GetKeySchema() const override { return key_schema_.get(); }
  page_id_t GetRootPageId() const override { return b_tree_->GetRootPageId(); }

private:
  std::string name_;
  std::unique_ptr<Schema> key_schema_;
  std::unique_ptr<BPlusTree<KeyType, ValueType, KeyComparator>> b_tree_;
};

} // namespace tetodb