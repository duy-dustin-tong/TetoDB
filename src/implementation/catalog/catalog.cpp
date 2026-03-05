// catalog.cpp

#include <fstream>
#include <sstream>

#include "catalog/catalog.h"
#include "common/config.h"

namespace tetodb {

bool Catalog::CreateTable(const std::string &table_name, const Schema &schema,
                          page_id_t root_page_id,
                          const std::vector<uint32_t> &primary_keys,
                          const std::vector<uint32_t> &unique_keys,
                          const std::vector<ForeignKeyDef> &fk_defs) {
  std::unique_lock<std::mutex> lock(latch_);

  for (const auto &pair : tables_) {
    if (pair.second->name_ == table_name)
      return false;
  }

  table_oid_t table_oid = next_table_oid_++;

  std::unique_ptr<TableHeap> table_heap;
  if (root_page_id == INVALID_PAGE_ID) {
    table_heap = std::make_unique<TableHeap>(bpm_, log_manager_, nullptr);
  } else {
    table_heap = std::make_unique<TableHeap>(bpm_, root_page_id, log_manager_);
  }

  auto table_meta = std::make_unique<TableMetadata>(
      schema, table_name, std::move(table_heap), table_oid);

  for (const auto &fk_ast : fk_defs) {
    TableMetadata *parent_meta = nullptr;
    for (const auto &pair : tables_) {
      if (pair.second->name_ == fk_ast.parent_table_) {
        parent_meta = pair.second.get();
        break;
      }
    }
    if (!parent_meta) {
      throw std::runtime_error("Catalog Error: Parent table '" +
                               fk_ast.parent_table_ + "' does not exist.");
    }

    uint32_t child_col_idx = table_meta->schema_.GetColIdx(fk_ast.child_col_);
    if (child_col_idx == static_cast<uint32_t>(-1)) {
      throw std::runtime_error("Catalog Error: Column '" + fk_ast.child_col_ +
                               "' not found in table '" + table_name + "'.");
    }

    uint32_t parent_col_idx =
        parent_meta->schema_.GetColIdx(fk_ast.parent_col_);
    if (parent_col_idx == static_cast<uint32_t>(-1)) {
      throw std::runtime_error("Catalog Error: Column '" + fk_ast.parent_col_ +
                               "' not found in parent table.");
    }

    std::vector<uint32_t> child_attrs = {child_col_idx};
    std::vector<uint32_t> parent_attrs = {parent_col_idx};
    std::string fk_name = "fk_" + table_name + "_" + fk_ast.parent_table_;

    ForeignKey fk(fk_name, child_attrs, parent_meta->oid_, parent_attrs,
                  fk_ast.on_delete_, fk_ast.on_update_);
    table_meta->foreign_keys_.push_back(fk);
  }

  tables_[table_oid] = std::move(table_meta);
  names_[table_name] = table_oid;

  lock.unlock();

  if (!primary_keys.empty()) {
    std::string pk_name = "pk_" + table_name;
    // --- UPDATED: Pass 'true' for is_unique ---
    CreateIndex(pk_name, table_oid, primary_keys, true, nullptr);
  }

  // --- NEW: Mount unique constraints! ---
  for (uint32_t unique_col : unique_keys) {
    std::string unq_name =
        "unique_" + table_name + "_" + schema.GetColumn(unique_col).GetName();
    CreateIndex(unq_name, table_oid, {unique_col}, true, nullptr);
  }

  SaveCatalog(catalog_path_);

  return true;
}

TableMetadata *Catalog::GetTable(const std::string &table_name) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = names_.find(table_name);
  if (it == names_.end())
    return nullptr;
  return tables_[it->second].get();
}

TableMetadata *Catalog::GetTable(table_oid_t table_oid) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = tables_.find(table_oid);
  if (it == tables_.end())
    return nullptr;
  return it->second.get();
}

std::vector<TableMetadata *> Catalog::GetAllTables() {
  std::lock_guard<std::mutex> lock(latch_);
  std::vector<TableMetadata *> result;
  for (const auto &pair : tables_) {
    result.push_back(pair.second.get());
  }
  return result;
}

// --- UPDATED: Added bool is_unique ---
IndexMetadata *Catalog::CreateIndex(const std::string &index_name,
                                    table_oid_t table_oid,
                                    const std::vector<uint32_t> &key_attrs,
                                    bool is_unique, Transaction *txn,
                                    page_id_t root_page_id) {

  std::unique_lock<std::mutex> lock(latch_);

  if (index_names_.find(index_name) != index_names_.end()) {
    return nullptr;
  }

  auto table_iter = tables_.find(table_oid);
  if (table_iter == tables_.end()) {
    return nullptr;
  }
  TableMetadata *table_meta = table_iter->second.get();

  std::vector<Column> key_cols;
  for (uint32_t col_idx : key_attrs) {
    key_cols.push_back(table_meta->schema_.GetColumn(col_idx));
  }

  auto key_schema = std::make_unique<Schema>(key_cols);

  uint32_t key_size = 0;
  for (const auto &col : key_schema->GetColumns()) {
    if (col.IsInlined()) {
      key_size += col.GetFixedLength();
    } else {
      key_size += col.GetStorageLimit();
    }
  }

  std::unique_ptr<Index> index = nullptr;

  TypeId key_type = key_schema->GetColumn(0).GetTypeId();

  if (key_size <= 4) {
    GenericComparator<4> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<4>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<4>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 8) {
    GenericComparator<8> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<8>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<8>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 16) {
    GenericComparator<16> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<16>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<16>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 32) {
    GenericComparator<32> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<32>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<32>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 64) {
    GenericComparator<64> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<64>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<64>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 128) {
    GenericComparator<128> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<128>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<128>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<128>, RID, GenericComparator<128>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else if (key_size <= 256) {
    GenericComparator<256> comparator(key_type);
    uint32_t leaf_max =
        (PAGE_SIZE - 28) / (sizeof(GenericKey<256>) + sizeof(RID)) - 1;
    uint32_t internal_max =
        (PAGE_SIZE - 24) / (sizeof(GenericKey<256>) + sizeof(page_id_t)) - 1;
    index = std::make_unique<
        BPlusTreeIndex<GenericKey<256>, RID, GenericComparator<256>>>(
        index_name, bpm_, comparator, leaf_max, internal_max,
        std::move(key_schema), root_page_id);
  } else {
    throw std::runtime_error(
        "Index key size exceeds maximum supported 256 bytes!");
  }

  index_oid_t oid = next_index_oid_++;

  // --- UPDATED: Pass is_unique to Metadata constructor ---
  auto index_meta = std::make_unique<IndexMetadata>(
      index_name, table_meta->oid_, std::move(index), oid, key_attrs,
      is_unique);

  IndexMetadata *result = index_meta.get();

  indexes_[oid] = std::move(index_meta);
  index_names_[index_name] = oid;
  table_indexes_[table_oid].push_back(result);

  lock.unlock();

  if (root_page_id == INVALID_PAGE_ID) {
    PopulateIndex(table_meta, result, txn);
  }

  return result;
}

// --- UPDATED: Added bool is_unique ---
IndexMetadata *
Catalog::CreateIndex(const std::string &index_name,
                     const std::string &table_name,
                     const std::vector<std::string> &column_names,
                     bool is_unique, Transaction *txn) {
  TableMetadata *table_meta = GetTable(table_name);
  if (!table_meta) {
    throw std::runtime_error("Catalog Error: Table '" + table_name +
                             "' not found.");
  }

  std::vector<uint32_t> key_attrs;
  for (const auto &col_name : column_names) {
    uint32_t col_idx = table_meta->schema_.GetColIdx(col_name);
    if (col_idx == static_cast<uint32_t>(-1)) {
      throw std::runtime_error("Catalog Error: Column '" + col_name +
                               "' not found.");
    }
    key_attrs.push_back(col_idx);
  }

  // --- UPDATED: Pass is_unique to base CreateIndex ---
  IndexMetadata *result =
      CreateIndex(index_name, table_meta->oid_, key_attrs, is_unique, txn);

  if (result) {
    SaveCatalog(catalog_path_);
  }
  return result;
}

IndexMetadata *Catalog::GetIndex(const std::string &index_name) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = index_names_.find(index_name);
  if (it == index_names_.end())
    return nullptr;
  return indexes_[it->second].get();
}

IndexMetadata *Catalog::GetIndex(index_oid_t index_oid) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = indexes_.find(index_oid);
  if (it == indexes_.end())
    return nullptr;
  return it->second.get();
}

std::vector<IndexMetadata *> Catalog::GetTableIndexes(table_oid_t table_oid) {
  std::lock_guard<std::mutex> lock(latch_);
  if (table_indexes_.find(table_oid) != table_indexes_.end()) {
    return table_indexes_[table_oid];
  }
  return {};
}

void Catalog::AddForeignKey(table_oid_t child_table_oid, const ForeignKey &fk) {
  std::unique_lock<std::mutex> lock(latch_);
  if (tables_.find(child_table_oid) != tables_.end()) {
    tables_[child_table_oid]->foreign_keys_.push_back(fk);
  }
}

void Catalog::PopulateIndex(TableMetadata *table_meta,
                            IndexMetadata *index_meta, Transaction *txn) {
  std::unique_lock<std::shared_mutex> table_lock(table_meta->table_latch_);

  // Use the caller's transaction, or create a local one for boot-time
  // index rebuilds (e.g., after crash recovery). A single Transaction
  // is safe because InsertIntoLeaf calls UnlockUnpinPages() after each
  // insert, which clears the page set between inserts.
  Transaction boot_txn(0);
  Transaction *effective_txn = (txn != nullptr) ? txn : &boot_txn;

  std::vector<Column> key_cols;
  for (uint32_t col_idx : index_meta->key_attrs_) {
    key_cols.push_back(table_meta->schema_.GetColumn(col_idx));
  }
  Schema key_schema(key_cols);

  auto iter = table_meta->table_->Begin();
  while (iter != table_meta->table_->End()) {
    RID rid = iter.GetRid();
    Tuple tuple;
    if (table_meta->table_->GetTuple(rid, &tuple, effective_txn)) {

      std::vector<Value> key_values;
      for (uint32_t col_idx : index_meta->key_attrs_) {
        key_values.push_back(tuple.GetValue(&table_meta->schema_, col_idx));
      }

      Tuple key_tuple(key_values, &key_schema);
      index_meta->index_->InsertEntry(key_tuple, rid, effective_txn);
    }
    ++iter;
  }
}

bool Catalog::DropTable(const std::string &table_name) {
  std::unique_lock<std::mutex> lock(latch_);

  auto it = names_.find(table_name);
  if (it == names_.end())
    return false;

  table_oid_t table_oid = it->second;

  for (const auto &[other_oid, other_meta] : tables_) {
    if (other_oid == table_oid)
      continue;

    for (const auto &fk : other_meta->foreign_keys_) {
      if (fk.parent_table_oid_ == table_oid) {
        throw std::runtime_error(
            "Execution Error: Cannot drop table '" + table_name +
            "'. It is referenced by a foreign key in table '" +
            other_meta->name_ + "'.");
      }
    }
  }

  auto index_it = table_indexes_.find(table_oid);
  if (index_it != table_indexes_.end()) {
    for (IndexMetadata *index_meta : index_it->second) {
      index_meta->index_->Destroy();

      index_names_.erase(index_meta->name_);
      indexes_.erase(index_meta->oid_);
    }
    table_indexes_.erase(index_it);
  }

  tables_[table_oid]->table_->Destroy();

  tables_.erase(table_oid);
  names_.erase(table_name);

  lock.unlock();

  SaveCatalog(catalog_path_);

  return true;
}

bool Catalog::DropIndex(const std::string &index_name) {
  std::unique_lock<std::mutex> lock(latch_);

  auto it = index_names_.find(index_name);
  if (it == index_names_.end())
    return false;

  index_oid_t index_oid = it->second;
  IndexMetadata *index_meta = indexes_[index_oid].get();
  table_oid_t table_oid = index_meta->table_oid_;

  // Destruct index from disk/heap mappings
  index_meta->index_->Destroy();

  // Remove from table_indexes mapping
  auto &table_idx_list = table_indexes_[table_oid];
  table_idx_list.erase(
      std::remove(table_idx_list.begin(), table_idx_list.end(), index_meta),
      table_idx_list.end());

  // Remove from master maps
  indexes_.erase(index_oid);
  index_names_.erase(index_name);

  lock.unlock();

  SaveCatalog(catalog_path_);

  return true;
}

bool Catalog::CreateView(const std::string &view_name,
                         std::unique_ptr<SelectStatement> query) {
  std::lock_guard<std::mutex> lock(latch_);
  if (views_.find(view_name) != views_.end()) {
    return false; // View already exists
  }
  views_[view_name] =
      std::make_unique<ViewMetadata>(view_name, std::move(query));
  return true;
}

ViewMetadata *Catalog::GetView(const std::string &view_name) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = views_.find(view_name);
  if (it == views_.end()) {
    return nullptr;
  }
  return it->second.get();
}

bool Catalog::DropView(const std::string &view_name) {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = views_.find(view_name);
  if (it == views_.end()) {
    return false; // View not found
  }
  views_.erase(it);
  return true;
}

void Catalog::SaveCatalog(const std::string &file_path) {
  std::lock_guard<std::mutex> lock(latch_);
  std::ofstream out(file_path);

  for (const auto &[oid, meta] : tables_) {
    out << "TABLE " << meta->name_ << " " << meta->table_->GetFirstPageId()
        << "\n";
    for (const auto &col : meta->schema_.GetColumns()) {
      out << "COLUMN " << col.GetName() << " "
          << static_cast<int>(col.GetTypeId()) << "\n";
    }
    out << "END\n";
  }

  for (const auto &[oid, meta] : indexes_) {
    // --- UPDATED: Serialize is_unique_ flag ---
    out << "INDEX " << meta->name_ << " " << meta->table_oid_ << " "
        << meta->index_->GetRootPageId() << " " << meta->key_attrs_.size()
        << " " << meta->is_unique_ << " ";
    for (uint32_t col_idx : meta->key_attrs_) {
      out << col_idx << " ";
    }
    out << "\n";
  }

  for (const auto &[oid, meta] : tables_) {
    for (const auto &fk : meta->foreign_keys_) {
      std::string parent_name = "";
      auto parent_it = tables_.find(fk.parent_table_oid_);
      if (parent_it != tables_.end()) {
        parent_name = parent_it->second->name_;
      }

      if (!parent_name.empty()) {
        out << "FK " << meta->name_ << " " << fk.fk_name_ << " " << parent_name
            << " " << fk.child_key_attrs_[0] << " " << fk.parent_key_attrs_[0]
            << " " << static_cast<int>(fk.on_delete_) << " "
            << static_cast<int>(fk.on_update_) << "\n";
      }
    }
  }
}

void Catalog::LoadCatalog(const std::string &file_path,
                          bool force_index_rebuild) {
  std::ifstream in(file_path);
  if (!in.is_open())
    return;

  std::string line;
  std::string current_table = "";
  int32_t current_first_page_id = -1;
  std::vector<Column> current_cols;

  while (std::getline(in, line)) {
    std::stringstream ss(line);
    std::string token;
    ss >> token;

    if (token == "TABLE") {
      ss >> current_table >> current_first_page_id;
      current_cols.clear();
    } else if (token == "COLUMN") {
      std::string col_name;
      int type_int;
      ss >> col_name >> type_int;
      current_cols.push_back(Column(col_name, static_cast<TypeId>(type_int)));
    } else if (token == "INDEX") {
      std::string idx_name;
      table_oid_t tbl_oid;
      page_id_t root_page_id;
      int num_attrs;
      bool is_unique = false; // --- UPDATED: Read is_unique_ flag ---

      ss >> idx_name >> tbl_oid >> root_page_id >> num_attrs >> is_unique;

      std::vector<uint32_t> attrs;
      for (int i = 0; i < num_attrs; ++i) {
        uint32_t attr;
        ss >> attr;
        attrs.push_back(attr);
      }

      TableMetadata *t_meta = GetTable(tbl_oid);
      if (t_meta) {
        // After crash recovery, B+Tree pages may be stale (recovery modified
        // table pages without updating indexes). Force a full rebuild.
        page_id_t effective_root =
            force_index_rebuild ? INVALID_PAGE_ID : root_page_id;
        CreateIndex(idx_name, t_meta->oid_, attrs, is_unique, nullptr,
                    effective_root);
      }
    } else if (token == "FK") {
      std::string child_table, fk_name, parent_table;
      uint32_t child_col_idx, parent_col_idx;
      int on_delete_int, on_update_int;

      ss >> child_table >> fk_name >> parent_table >> child_col_idx >>
          parent_col_idx >> on_delete_int >> on_update_int;

      TableMetadata *c_meta = GetTable(child_table);
      TableMetadata *p_meta = GetTable(parent_table);

      if (c_meta && p_meta) {
        std::vector<uint32_t> c_attrs = {child_col_idx};
        std::vector<uint32_t> p_attrs = {parent_col_idx};

        ForeignKey fk(fk_name, c_attrs, p_meta->oid_, p_attrs,
                      static_cast<ReferentialAction>(on_delete_int),
                      static_cast<ReferentialAction>(on_update_int));

        AddForeignKey(c_meta->oid_, fk);
      }
    } else if (token == "END") {
      Schema schema(current_cols);
      CreateTable(current_table, schema, current_first_page_id,
                  std::vector<uint32_t>{}, std::vector<uint32_t>{},
                  std::vector<ForeignKeyDef>{});
    }
  }
}

} // namespace tetodb