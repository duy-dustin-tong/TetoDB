// catalog.h

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <vector>

#include "storage/buffer/buffer_pool_manager.h"
#include "storage/table/table_heap.h"
#include "catalog/schema.h"
#include "index/index.h"
#include "index/b_plus_tree_index.h"
#include "recovery/log_manager.h" // <-- NEW: Include LogManager
#include "parser/ast.h"



namespace tetodb {

    class Transaction; // Forward declaration

    using table_oid_t = uint32_t;
    using index_oid_t = uint32_t;


    /**
     * ForeignKey maps a set of columns in a Child table to a set of columns in a Parent table.
     */
    struct ForeignKey {
        std::string fk_name_;
        std::vector<uint32_t> child_key_attrs_;  // Column indexes in THIS table
        table_oid_t parent_table_oid_;           // OID of the referenced table
        std::vector<uint32_t> parent_key_attrs_; // Column indexes in the PARENT table
        ReferentialAction on_delete_;
        ReferentialAction on_update_;

        ForeignKey(std::string fk_name, std::vector<uint32_t> child_attrs,
            table_oid_t parent_oid, std::vector<uint32_t> parent_attrs,
            ReferentialAction on_delete = ReferentialAction::RESTRICT,
            ReferentialAction on_update = ReferentialAction::RESTRICT)
            : fk_name_(std::move(fk_name)), child_key_attrs_(std::move(child_attrs)),
            parent_table_oid_(parent_oid), parent_key_attrs_(std::move(parent_attrs)),
            on_delete_(on_delete), on_update_(on_update) {
        }
    };


    /**
     * TableMetadata: A container for table information.
     */
    struct TableMetadata {
        TableMetadata(Schema schema, std::string name, std::unique_ptr<TableHeap> table, table_oid_t oid)
            : schema_(std::move(schema)), name_(std::move(name)),
            table_(std::move(table)), oid_(oid) {
        }

        Schema schema_;
        std::string name_;
        std::unique_ptr<TableHeap> table_;
        std::vector<ForeignKey> foreign_keys_;
        std::shared_mutex table_latch_;

        table_oid_t oid_;
    };

    /**
     * IndexMetadata: A container for index information.
     */
    struct IndexMetadata {
        IndexMetadata(std::string name, table_oid_t table_oid,
            std::unique_ptr<Index> index, index_oid_t oid,
            std::vector<uint32_t> key_attrs, bool is_unique) // <-- ADDED
            : name_(std::move(name)), table_oid_(table_oid),
            index_(std::move(index)), oid_(oid), key_attrs_(std::move(key_attrs)),
            is_unique_(is_unique) {
        } // <-- ADDED

        std::string name_;
        table_oid_t table_oid_;
        std::unique_ptr<Index> index_;
        index_oid_t oid_;
        std::vector<uint32_t> key_attrs_;
        bool is_unique_; // <-- ADDED
    };

    

    

    /**
     * Catalog: The "Phonebook" of the database.
     */
    class Catalog {
    public:
        // UPDATED: Added LogManager pointer with a default nullptr
        explicit Catalog(BufferPoolManager* bpm, LogManager* log_manager = nullptr)
            : bpm_(bpm), log_manager_(log_manager) {
        }

        bool CreateTable(const std::string& table_name, const Schema& schema,
            page_id_t root_page_id, const std::vector<uint32_t>& primary_keys,
            const std::vector<ForeignKeyDef>& fk_defs = {});
        TableMetadata* GetTable(const std::string& table_name);
        TableMetadata* GetTable(table_oid_t table_oid);
        std::vector<TableMetadata*> GetAllTables();

        IndexMetadata* CreateIndex(const std::string& index_name, table_oid_t table_oid,
            const std::vector<uint32_t>& key_attrs, bool is_unique, // <-- ADDED
            Transaction* txn, page_id_t root_page_id = INVALID_PAGE_ID);

        IndexMetadata* CreateIndex(const std::string& index_name, const std::string& table_name,
            const std::vector<std::string>& column_names, bool is_unique, // <-- ADDED
            Transaction* txn);

        IndexMetadata* GetIndex(const std::string& index_name);
        IndexMetadata* GetIndex(index_oid_t index_oid);
        std::vector<IndexMetadata*> GetTableIndexes(table_oid_t table_oid);

        void AddForeignKey(table_oid_t child_table_oid, const ForeignKey& fk);

        bool DropTable(const std::string& table_name);

        void SaveCatalog(const std::string& file_path);
        void LoadCatalog(const std::string& file_path);

    private:
        void PopulateIndex(TableMetadata* table_meta, IndexMetadata* index_meta, Transaction* txn);

    private:
        BufferPoolManager* bpm_;
        LogManager* log_manager_; // <-- NEW: Store the LogManager pointer
        std::mutex latch_;

        std::atomic<table_oid_t> next_table_oid_{ 0 };
        std::unordered_map<table_oid_t, std::unique_ptr<TableMetadata>> tables_;
        std::unordered_map<std::string, table_oid_t> names_;

        std::atomic<index_oid_t> next_index_oid_{ 0 };
        std::unordered_map<index_oid_t, std::unique_ptr<IndexMetadata>> indexes_;
        std::unordered_map<table_oid_t, std::vector<IndexMetadata*>> table_indexes_;
        std::unordered_map<std::string, index_oid_t> index_names_;
    };

} // namespace tetodb