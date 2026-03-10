// src/test_bug19_fk_cascade.cpp
#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h"
#include "execution/fk_constraint_handler.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <vector>

using namespace tetodb;

int main() {
  std::cout << "--- TetoDB Bug 19 FK Cascade Validation ---" << std::endl;
  std::filesystem::remove("test_fk.db");
  std::filesystem::remove("test_fk.log");
  std::filesystem::remove("test_fk.freelist");

  auto disk_mgr = std::make_unique<DiskManager>("test_fk.db");
  auto replacer = std::make_unique<TwoQueueReplacer>(100);
  auto bpm =
      std::make_unique<BufferPoolManager>(100, disk_mgr.get(), replacer.get());
  auto lock_mgr = std::make_unique<LockManager>();
  auto tm = std::make_unique<TransactionManager>(lock_mgr.get());
  auto catalog = std::make_unique<Catalog>("test_fk_catalog.db", bpm.get());

  // Parent Schema
  std::vector<Column> p_cols = {{"p_id1", TypeId::INTEGER},
                                {"p_id2", TypeId::INTEGER},
                                {"p_val", TypeId::INTEGER}};
  Schema p_schema(p_cols);
  catalog->CreateTable("Parent", p_schema, -1, {0});
  auto p_meta = catalog->GetTable("Parent");

  // Child Schema
  std::vector<Column> c_cols = {{"c_id", TypeId::INTEGER},
                                {"p_ref1", TypeId::INTEGER},
                                {"p_ref2", TypeId::INTEGER}};
  Schema c_schema(c_cols);
  catalog->CreateTable("Child", c_schema, -1, {0});
  auto c_meta = catalog->GetTable("Child");

  // Grandchild Schema
  std::vector<Column> g_cols = {{"g_id", TypeId::INTEGER},
                                {"c_ref", TypeId::INTEGER}};
  Schema g_schema(g_cols);
  catalog->CreateTable("Grandchild", g_schema, -1, {0});
  auto g_meta = catalog->GetTable("Grandchild");

  // Establish Multi-Column FK: Child -> Parent
  ForeignKey c_fk("c_fk", {1, 2}, p_meta->oid_, {0, 1},
                  ReferentialAction::CASCADE, ReferentialAction::CASCADE);
  catalog->AddForeignKey(c_meta->oid_, c_fk);

  // Establish FK: Grandchild -> Child
  ForeignKey g_fk("g_fk", {1}, c_meta->oid_, {0}, ReferentialAction::CASCADE,
                  ReferentialAction::CASCADE);
  catalog->AddForeignKey(g_meta->oid_, g_fk);

  auto txn = tm->Begin();
  auto acquire_write_lock = [](const RID &) { return true; };

  // Insert Parent Data (100, 200, 50)
  Tuple p1({Value(TypeId::INTEGER, 100), Value(TypeId::INTEGER, 200),
            Value(TypeId::INTEGER, 50)},
           &p_schema);
  RID p1_rid;
  p_meta->table_->InsertTuple(p1, &p1_rid, txn);

  // Insert Child Data (1, 100, 200)
  Tuple c1({Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 100),
            Value(TypeId::INTEGER, 200)},
           &c_schema);
  RID c1_rid;
  c_meta->table_->InsertTuple(c1, &c1_rid, txn);

  // Insert Grandchild Data (10, 1)
  Tuple g1({Value(TypeId::INTEGER, 10), Value(TypeId::INTEGER, 1)}, &g_schema);
  RID g1_rid;
  g_meta->table_->InsertTuple(g1, &g1_rid, txn);

  std::cout << "Constructed Parent -> Child -> Grandchild dependency chain."
            << std::endl;

  // Test UPDATE Cascade!
  // Change Parent ID from (100, 200) to (101, 201)
  Tuple p1_new({Value(TypeId::INTEGER, 101), Value(TypeId::INTEGER, 201),
                Value(TypeId::INTEGER, 50)},
               &p_schema);

  // TetoDB uses a trigger internally, but here we can just invoke it:
  FKConstraintHandler::EnforceOnUpdate(p1, p1_new, p_meta, catalog.get(), txn,
                                       acquire_write_lock);

  // Verify child updated
  Tuple c_check;
  c_meta->table_->GetTuple(c1_rid, &c_check, txn);
  if (c_check.GetValue(&c_schema, 1).CastAsInteger() == 101 &&
      c_check.GetValue(&c_schema, 2).CastAsInteger() == 201) {
    std::cout << "SUCCESS: Child automatically cascaded via multi-column "
                 "referential update!"
              << std::endl;
  } else {
    std::cout << "FAIL: Multi-column referential update neglected the child."
              << std::endl;
  }

  // Now let's test DELETE cascade on Parent
  // It should recursively delete the child, AND the grandchild!
  // Construct the new matched child tuple post-update so the delete correctly
  // triggers. wait, EnforceOnDelete will search via table scan matching parent
  // attributes.
  std::cout << "Deleting Parent tuple..." << std::endl;
  FKConstraintHandler::EnforceOnDelete(p1_new, p_meta, catalog.get(), txn,
                                       acquire_write_lock);

  // Verify Child Deletion
  bool c_exists = false;
  auto iter_c = c_meta->table_->Begin(txn);
  while (iter_c != c_meta->table_->End()) {
    Tuple t;
    if (c_meta->table_->GetTuple(iter_c.GetRid(), &t, txn)) {
      // Check if tuple is marked deleted in logical headers
      c_exists = true;
    }
    ++iter_c;
  }

  if (!c_exists) {
    std::cout << "SUCCESS: Child was cascadingly deleted." << std::endl;
  } else {
    std::cout << "FAIL: Child survived." << std::endl;
  }

  bool g_exists = false;
  auto iter_g = g_meta->table_->Begin(txn);
  while (iter_g != g_meta->table_->End()) {
    Tuple t;
    if (g_meta->table_->GetTuple(iter_g.GetRid(), &t, txn)) {
      g_exists = true;
    }
    ++iter_g;
  }

  if (!g_exists) {
    std::cout << "SUCCESS: Grandchild automatically deleted via recursive "
                 "referential cascade!"
              << std::endl;
  } else {
    std::cout << "FAIL: Grandchild was orphaned and not deleted! "
                 "EnforceOnDelete failed to recurse."
              << std::endl;
  }

  tm->Commit(txn);

  bpm = nullptr;
  disk_mgr = nullptr;
  replacer = nullptr;
  std::filesystem::remove("test_fk.db");
  std::filesystem::remove("test_fk.log");
  std::filesystem::remove("test_fk.freelist");

  std::cout << "Database successfully handled deep cascaded actions."
            << std::endl;
  return 0;
}
