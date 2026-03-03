// b_plus_tree_index.h

#pragma once

#include "index/b_plus_tree.h"
#include "index/generic_key.h"
#include "index/index.h"


namespace tetodb {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>

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