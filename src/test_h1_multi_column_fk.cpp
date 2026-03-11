// test_h1_multi_column_fk.cpp
#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "execution/plans/insert_plan.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace tetodb;

int main() {
    std::cout << "--- TetoDB H1 Multi-Column FK Validation ---" << std::endl;
    std::filesystem::remove("test_h1.db");
    std::filesystem::remove("test_h1.log");
    std::filesystem::remove("test_h1.freelist");
    std::filesystem::remove("test_h1_catalog.db");

    auto disk_mgr = std::make_unique<DiskManager>("test_h1.db");
    auto replacer = std::make_unique<TwoQueueReplacer>(100);
    auto bpm = std::make_unique<BufferPoolManager>(100, disk_mgr.get(), replacer.get());
    auto lock_mgr = std::make_unique<LockManager>();
    auto tm = std::make_unique<TransactionManager>(lock_mgr.get());
    auto catalog = std::make_unique<Catalog>("test_h1_catalog.db", bpm.get());

    // Parent Schema
    std::vector<Column> p_cols = {{"p_id1", TypeId::INTEGER}, {"p_id2", TypeId::INTEGER}};
    Schema p_schema(p_cols);
    catalog->CreateTable("Parent", p_schema, -1, {0, 1}); // Multi-col PK
    auto p_meta = catalog->GetTable("Parent");

    // Child Schema
    std::vector<Column> c_cols = {{"c_id", TypeId::INTEGER}, {"c_ref1", TypeId::INTEGER}, {"c_ref2", TypeId::INTEGER}};
    Schema c_schema(c_cols);
    catalog->CreateTable("Child", c_schema, -1, {0}); // PK on c_id
    auto c_meta = catalog->GetTable("Child");

    // Multi-Column FK: Child -> Parent
    ForeignKey c_fk("c_fk", {1, 2}, p_meta->oid_, {0, 1}, ReferentialAction::RESTRICT, ReferentialAction::RESTRICT);
    catalog->AddForeignKey(c_meta->oid_, c_fk);

    auto txn = tm->Begin();

    // Directly insert parent tuple (1, 2)
    Tuple p1({Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 2)}, &p_schema);
    RID p1_rid;
    p_meta->table_->InsertTuple(p1, &p1_rid, txn, lock_mgr.get());
    
    auto p_indexes = catalog->GetTableIndexes(p_meta->oid_);
    p_indexes[0]->index_->InsertEntry(p1, p1_rid, txn);

    // Parent tuple (5, 6)
    Tuple p2({Value(TypeId::INTEGER, 5), Value(TypeId::INTEGER, 6)}, &p_schema);
    RID p2_rid;
    p_meta->table_->InsertTuple(p2, &p2_rid, txn, lock_mgr.get());
    p_indexes[0]->index_->InsertEntry(p2, p2_rid, txn);

    ExecutionContext exec_ctx(catalog.get(), bpm.get(), txn, lock_mgr.get(), tm.get());

    auto insert_child = [&](int cid, int ref1, int ref2) -> bool {
        ConstantValueExpression cv1(Value(TypeId::INTEGER, cid));
        ConstantValueExpression cv2(Value(TypeId::INTEGER, ref1));
        ConstantValueExpression cv3(Value(TypeId::INTEGER, ref2));
        std::vector<const AbstractExpression*> row = {&cv1, &cv2, &cv3};
        std::vector<std::vector<const AbstractExpression*>> raw_exprs = {row};
        InsertPlanNode plan(c_meta->oid_, raw_exprs);
        
        auto executor = ExecutionEngine::CreateExecutor(&plan, &exec_ctx);
        executor->Init();
        Tuple ignored; RID ignored_rid;
        try {
            return executor->Next(&ignored, &ignored_rid);
        } catch (const std::exception& e) {
            std::cout << "Caught: " << e.what() << std::endl;
            return false;
        }
    };

    std::cout << "\n[Test 1] Insert (10, 1, 2) - should match parent (1, 2)" << std::endl;
    bool res1 = insert_child(10, 1, 2);
    assert(res1 && "Valid FK insert must succeed");

    std::cout << "\n[Test 2] Insert (11, 1, 3) - partial match, MUST fail" << std::endl;
    bool res2 = insert_child(11, 1, 3);
    assert(!res2 && "Invalid multi-column FK insert must fail");

    std::cout << "\n[Test 3] Insert (12, 3, 4) - mismatch, MUST fail" << std::endl;
    bool res3 = insert_child(12, 3, 4);
    assert(!res3 && "Invalid FK insert must fail");

    std::cout << "\nPASS: Multi-column FK validation is fully functional" << std::endl;

    tm->Abort(txn);
    
    // Explicitly destroy objects to release file handles before deleting
    catalog.reset();
    tm.reset();
    lock_mgr.reset();
    bpm.reset();
    replacer.reset();
    disk_mgr.reset();

    std::error_code ec;
    std::filesystem::remove("test_h1.db", ec);
    std::filesystem::remove("test_h1.log", ec);
    std::filesystem::remove("test_h1.freelist", ec);
    std::filesystem::remove("test_h1_catalog.db", ec);
    
    return 0;
}
