// test_savepoint.cpp — GTest suite for SAVEPOINT / RELEASE / ROLLBACK TO

#include <gtest/gtest.h>
#include <filesystem>
#include "server/tetodb_instance.h"

using namespace tetodb;

static const std::string TEST_DB = "test_savepoint.db";

class SavepointTest : public ::testing::Test {
protected:
  static void SetUpTestSuite() {
    auto data_dir = std::filesystem::path("data_test_savepoint.db");
    if (std::filesystem::exists(data_dir)) {
      std::filesystem::remove_all(data_dir);
    }
    for (auto &ext : {".catalog", ".log"}) {
      auto f = std::filesystem::path("test_savepoint" + std::string(ext));
      if (std::filesystem::exists(f)) std::filesystem::remove(f);
    }
    db_ = std::make_unique<TetoDBInstance>(TEST_DB);
  }

  static void TearDownTestSuite() {
    db_.reset();
    auto data_dir = std::filesystem::path("data_test_savepoint.db");
    if (std::filesystem::exists(data_dir)) {
      std::filesystem::remove_all(data_dir);
    }
    for (auto &ext : {".catalog", ".log"}) {
      auto f = std::filesystem::path("test_savepoint" + std::string(ext));
      if (std::filesystem::exists(f)) std::filesystem::remove(f);
    }
  }

  void SetUp() override {
    session_ = ClientSession{};
  }

  QueryResult Exec(const std::string &sql) {
    return db_->ExecuteQuery(sql, session_);
  }

  int CountRows(const std::string &table) {
    auto res = Exec("SELECT COUNT(*) FROM " + table + ";");
    if (res.is_error || res.rows.empty()) return -1;
    return res.rows[0].GetValue(res.schema, 0).GetAsInteger();
  }

  static std::unique_ptr<TetoDBInstance> db_;
  ClientSession session_;
};

std::unique_ptr<TetoDBInstance> SavepointTest::db_ = nullptr;

// =============================================================
// 1. Basic SAVEPOINT + ROLLBACK TO
// =============================================================
TEST_F(SavepointTest, BasicRollbackTo) {
  Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER);");
  Exec("BEGIN;");
  Exec("INSERT INTO t1 VALUES (1, 100);");

  auto sp_res = Exec("SAVEPOINT sp1;");
  EXPECT_EQ(sp_res.status_msg, "SAVEPOINT");

  Exec("INSERT INTO t1 VALUES (2, 200);");
  Exec("INSERT INTO t1 VALUES (3, 300);");

  EXPECT_EQ(CountRows("t1"), 3);

  auto rb_res = Exec("ROLLBACK TO sp1;");
  EXPECT_EQ(rb_res.status_msg, "ROLLBACK");

  EXPECT_EQ(CountRows("t1"), 1);

  Exec("COMMIT;");
  EXPECT_EQ(CountRows("t1"), 1);
  Exec("DROP TABLE t1;");
}

// =============================================================
// 2. RELEASE SAVEPOINT — then ROLLBACK TO should fail
// =============================================================
TEST_F(SavepointTest, ReleaseSavepoint) {
  Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY);");
  Exec("BEGIN;");
  Exec("INSERT INTO t2 VALUES (1);");
  Exec("SAVEPOINT sp1;");
  Exec("INSERT INTO t2 VALUES (2);");

  auto rel_res = Exec("RELEASE SAVEPOINT sp1;");
  EXPECT_EQ(rel_res.status_msg, "RELEASE");

  // ROLLBACK TO released savepoint should fail (poisons the transaction)
  auto fail_res = Exec("ROLLBACK TO sp1;");
  EXPECT_TRUE(fail_res.is_error);

  // The transaction is now poisoned — COMMIT will become ROLLBACK
  auto commit_res = Exec("COMMIT;");
  // After the poisoned commit (which aborts), both rows are gone
  EXPECT_EQ(commit_res.status_msg, "ROLLBACK");

  // Table is empty since the entire transaction was aborted
  EXPECT_EQ(CountRows("t2"), 0);
  Exec("DROP TABLE t2;");
}

// =============================================================
// 3. Nested savepoints — ROLLBACK TO earlier one
// =============================================================
TEST_F(SavepointTest, NestedSavepoints) {
  Exec("CREATE TABLE t3 (id INTEGER PRIMARY KEY, val INTEGER);");
  Exec("BEGIN;");
  Exec("INSERT INTO t3 VALUES (1, 10);");

  Exec("SAVEPOINT sp1;");
  Exec("INSERT INTO t3 VALUES (2, 20);");

  Exec("SAVEPOINT sp2;");
  Exec("INSERT INTO t3 VALUES (3, 30);");

  EXPECT_EQ(CountRows("t3"), 3);

  // Rollback to sp1 — should undo rows 2 and 3 (and destroy sp2)
  Exec("ROLLBACK TO sp1;");
  EXPECT_EQ(CountRows("t3"), 1);

  // sp2 should no longer exist
  auto fail_res = Exec("ROLLBACK TO sp2;");
  EXPECT_TRUE(fail_res.is_error);

  // The failed ROLLBACK TO sp2 poisons the transaction
  // So we need to ROLLBACK to clean up
  Exec("ROLLBACK;");

  // After full rollback, all data from the transaction is gone (row 1 too)
  EXPECT_EQ(CountRows("t3"), 0);
  Exec("DROP TABLE t3;");
}

// =============================================================
// 4. Savepoint without active transaction should fail
// =============================================================
TEST_F(SavepointTest, SavepointWithoutTransaction) {
  Exec("CREATE TABLE t4 (id INTEGER PRIMARY KEY);");
  auto res = Exec("SAVEPOINT sp1;");
  EXPECT_TRUE(res.is_error);
  Exec("DROP TABLE t4;");
}

// =============================================================
// 5. COMMIT after partial rollback persists pre-savepoint data
// =============================================================
TEST_F(SavepointTest, CommitAfterPartialRollback) {
  Exec("CREATE TABLE t5 (id INTEGER PRIMARY KEY, val INTEGER);");
  Exec("BEGIN;");
  Exec("INSERT INTO t5 VALUES (1, 100);");
  Exec("INSERT INTO t5 VALUES (2, 200);");

  Exec("SAVEPOINT sp1;");
  Exec("INSERT INTO t5 VALUES (3, 300);");

  Exec("ROLLBACK TO sp1;");
  EXPECT_EQ(CountRows("t5"), 2);

  // Insert new data after rollback
  Exec("INSERT INTO t5 VALUES (4, 400);");
  EXPECT_EQ(CountRows("t5"), 3);

  Exec("COMMIT;");
  EXPECT_EQ(CountRows("t5"), 3);
  Exec("DROP TABLE t5;");
}

// =============================================================
// 6. Nested savepoints — ROLLBACK TO inner, keep outer data
// =============================================================
TEST_F(SavepointTest, NestedRollbackToInner) {
  Exec("CREATE TABLE t6 (id INTEGER PRIMARY KEY, val INTEGER);");
  Exec("BEGIN;");
  Exec("INSERT INTO t6 VALUES (1, 10);");

  Exec("SAVEPOINT sp1;");
  Exec("INSERT INTO t6 VALUES (2, 20);");

  Exec("SAVEPOINT sp2;");
  Exec("INSERT INTO t6 VALUES (3, 30);");

  // Rollback to sp2 only — should undo row 3 but keep rows 1 and 2
  Exec("ROLLBACK TO sp2;");
  EXPECT_EQ(CountRows("t6"), 2);

  // sp1 should still be valid — rollback to it
  Exec("ROLLBACK TO sp1;");
  EXPECT_EQ(CountRows("t6"), 1);

  Exec("COMMIT;");
  EXPECT_EQ(CountRows("t6"), 1);
  Exec("DROP TABLE t6;");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
