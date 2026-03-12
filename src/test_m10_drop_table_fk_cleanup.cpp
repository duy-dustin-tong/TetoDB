// test_m10_drop_table_fk_cleanup.cpp
#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace tetodb;

int main() {
    std::cout << "--- TetoDB M10 DropTable FK Cleanup Validation ---" << std::endl;
    std::filesystem::remove("test_m10.db");
    std::filesystem::remove("test_m10.log");
    std::filesystem::remove("test_m10.freelist");
    std::filesystem::remove("test_m10_catalog.db");

    auto disk_mgr = std::make_unique<DiskManager>("test_m10.db");
    auto replacer = std::make_unique<TwoQueueReplacer>(100);
    auto bpm = std::make_unique<BufferPoolManager>(100, disk_mgr.get(), replacer.get());
    auto lock_mgr = std::make_unique<LockManager>();
    auto tm = std::make_unique<TransactionManager>(lock_mgr.get());
    auto catalog = std::make_unique<Catalog>("test_m10_catalog.db", bpm.get());

    // Parent Schema
    std::vector<Column> p_cols = {{"p_id", TypeId::INTEGER}};
    Schema p_schema(p_cols);
    catalog->CreateTable("Parent", p_schema, -1, {0}); // PK on p_id
    auto p_meta = catalog->GetTable("Parent");

    // Child Schema
    std::vector<Column> c_cols = {{"c_id", TypeId::INTEGER}, {"c_ref", TypeId::INTEGER}};
    Schema c_schema(c_cols);
    catalog->CreateTable("Child", c_schema, -1, {0}); // PK on c_id
    auto c_meta = catalog->GetTable("Child");

    // Foreign Key: Child -> Parent
    ForeignKey c_fk("c_fk", {1}, p_meta->oid_, {0}, ReferentialAction::RESTRICT, ReferentialAction::RESTRICT);
    catalog->AddForeignKey(c_meta->oid_, c_fk);

    // After adding the FK, the catalog should have generated an index "c_fk_idx".
    auto c_indexes = catalog->GetTableIndexes(c_meta->oid_);
    assert(c_indexes.size() == 2 && "Child table should have 2 indexes (1 PK + 1 FK)");

    bool fk_index_exists = false;
    for (auto* idx : c_indexes) {
        if (idx->name_ == "c_fk_idx") fk_index_exists = true;
    }
    assert(fk_index_exists && "FK index should exist before DropTable");

    std::cout << "[PASS] FK correctly generated an index natively." << std::endl;

    // Drop the Child Table
    bool drop_success = catalog->DropTable("Child");
    assert(drop_success && "Dropping child table should succeed");

    std::cout << "[PASS] Child Table Successfully Dropped." << std::endl;

    // The Catalog should now NOT contain the c_fk_idx index.
    auto child_indexes_after_drop = catalog->GetTableIndexes(c_meta->oid_);
    assert(child_indexes_after_drop.size() == 0 && "Child table should have 0 indexes after being dropped");

    // And trying to retrieve the index by name should fail (return null or crash)
    // There isn't a direct "GetIndexByName" exposed in the header usually, but if the indexes are actually dropped, we should be able to drop the Parent table safely now.
    
    bool parent_drop_success = catalog->DropTable("Parent");
    assert(parent_drop_success && "Dropping Parent table should succeed because Child is gone and FKs were scrubbed");
    
    std::cout << "[PASS] Parent Table Successfully Dropped, proving no stale incoming FK references!" << std::endl;

    std::cout << "--- M10 Verification Completed Successfully ---" << std::endl;
    return 0;
}
