// test_bug20_arithmetic.cpp
// Microbenchmark: Verify arithmetic expressions (+, -, *, /) in
// parser/planner/execution

#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_engine.h"
#include "parser/ast.h"
#include "parser/lexer.h"
#include "parser/parser.h"
#include "planner/planner.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"
#include <cassert>
#include <filesystem>
#include <iostream>

using namespace tetodb;

int main() {
  std::cout << "=== Bug 20: Arithmetic Expression Test ===" << std::endl;

  // --- 1. Test Lexer tokenizes arithmetic symbols ---
  {
    Lexer lex("SELECT price + tax * 2 - discount / 4 FROM products");
    auto tokens = lex.TokenizeAll();
    bool found_plus = false, found_minus = false, found_star = false,
         found_slash = false;
    for (const auto &t : tokens) {
      if (t.type_ == TokenType::SYMBOL && t.value_ == "+")
        found_plus = true;
      if (t.type_ == TokenType::SYMBOL && t.value_ == "-")
        found_minus = true;
      if (t.type_ == TokenType::SYMBOL && t.value_ == "*")
        found_star = true;
      if (t.type_ == TokenType::SYMBOL && t.value_ == "/")
        found_slash = true;
    }
    assert(found_plus && "Lexer must tokenize +");
    assert(found_minus && "Lexer must tokenize -");
    assert(found_star && "Lexer must tokenize *");
    assert(found_slash && "Lexer must tokenize /");
    std::cout << "[PASS] Lexer correctly tokenizes +, -, *, /" << std::endl;
  }

  // --- 2. Test Parser produces correct AST with precedence ---
  {
    // a + b * c should parse as: a + (b * c), NOT (a + b) * c
    Lexer lex("SELECT a + b * c FROM t");
    auto tokens = lex.TokenizeAll();
    Parser parser(tokens);
    auto ast = parser.ParseStatement();

    auto *select = dynamic_cast<SelectStatement *>(ast.get());
    assert(select && "Must parse as SELECT");
    assert(select->select_list_.size() == 1);

    // The top-level expression should be BinaryExpr with op "+"
    auto *top = dynamic_cast<BinaryExpr *>(select->select_list_[0].get());
    assert(top && "Top expression must be BinaryExpr");
    assert(top->op_ == "+" && "Top operator must be + (additive binds looser)");

    // The right child should be BinaryExpr with op "*"
    auto *right = dynamic_cast<BinaryExpr *>(top->right_.get());
    assert(right && "Right of + must be BinaryExpr");
    assert(right->op_ == "*" &&
           "Right operator must be * (multiplicative binds tighter)");

    std::cout << "[PASS] Parser respects PEMDAS: a + b * c => a + (b * c)"
              << std::endl;
  }

  // --- 3. Test unary negation ---
  {
    Lexer lex("SELECT -5 FROM t");
    auto tokens = lex.TokenizeAll();
    Parser parser(tokens);
    auto ast = parser.ParseStatement();

    auto *select = dynamic_cast<SelectStatement *>(ast.get());
    assert(select);
    // -5 is represented as BinaryExpr(0, "-", 5)
    auto *neg = dynamic_cast<BinaryExpr *>(select->select_list_[0].get());
    assert(neg && neg->op_ == "-" &&
           "Unary minus must produce BinaryExpr with -");
    std::cout << "[PASS] Unary negation -5 parsed correctly" << std::endl;
  }

  // --- 4. Test WHERE clause with arithmetic ---
  {
    Lexer lex("SELECT id FROM t WHERE age + 1 > 30");
    auto tokens = lex.TokenizeAll();
    Parser parser(tokens);
    auto ast = parser.ParseStatement();

    auto *select = dynamic_cast<SelectStatement *>(ast.get());
    assert(select && select->where_clause_);
    // WHERE should be: BinaryExpr(age + 1, ">", 30)
    auto *cmp = dynamic_cast<BinaryExpr *>(select->where_clause_.get());
    assert(cmp && cmp->op_ == ">" && "WHERE comparison must be >");

    auto *add_expr = dynamic_cast<BinaryExpr *>(cmp->left_.get());
    assert(add_expr && add_expr->op_ == "+" && "Left of > must be addition");
    std::cout << "[PASS] WHERE age + 1 > 30 parsed correctly" << std::endl;
  }

  // --- 5. Test full planner + execution integration ---
  {
    const std::string db_path = "test_bug20.db";
    std::filesystem::remove(db_path);
    std::filesystem::remove("test_bug20.freelist");
    std::filesystem::remove("test_bug20_catalog.db");

    auto disk_mgr = std::make_unique<DiskManager>(db_path);
    auto replacer = std::make_unique<TwoQueueReplacer>(100);
    auto bpm = std::make_unique<BufferPoolManager>(100, disk_mgr.get(),
                                                   replacer.get());
    auto lock_mgr = std::make_unique<LockManager>();
    auto txn_mgr = std::make_unique<TransactionManager>(lock_mgr.get());
    auto catalog =
        std::make_unique<Catalog>("test_bug20_catalog.db", bpm.get());
    Transaction *txn = txn_mgr->Begin();

    // Create products table: (id INT PK, price INT, qty INT)
    Schema schema({Column("id", TypeId::INTEGER),
                   Column("price", TypeId::INTEGER),
                   Column("qty", TypeId::INTEGER)});
    catalog->CreateTable("products", schema, -1, {0});
    TableMetadata *tbl = catalog->GetTable("products");
    assert(tbl);

    // Insert rows: (1, 10, 5), (2, 20, 3), (3, 15, 8)
    std::vector<std::vector<Value>> rows = {
        {Value(TypeId::INTEGER, 1), Value(TypeId::INTEGER, 10),
         Value(TypeId::INTEGER, 5)},
        {Value(TypeId::INTEGER, 2), Value(TypeId::INTEGER, 20),
         Value(TypeId::INTEGER, 3)},
        {Value(TypeId::INTEGER, 3), Value(TypeId::INTEGER, 15),
         Value(TypeId::INTEGER, 8)}};
    for (auto &row : rows) {
      Tuple tuple(row, &schema);
      RID rid;
      tbl->table_->InsertTuple(tuple, &rid, txn);
    }

    // Plan: SELECT price * qty FROM products
    Lexer lex("SELECT price * qty FROM products");
    auto tokens = lex.TokenizeAll();
    Parser parser(tokens);
    auto ast = parser.ParseStatement();

    ExecutionContext exec_ctx(catalog.get(), bpm.get(), txn, lock_mgr.get(),
                              txn_mgr.get());
    Planner planner(catalog.get(), &exec_ctx);
    auto *plan = planner.PlanQuery(ast.get());
    assert(plan);

    auto executor = ExecutionEngine::CreateExecutor(plan, &exec_ctx);
    executor->Init();

    Tuple tuple;
    RID rid;
    std::vector<double> results;
    while (executor->Next(&tuple, &rid)) {
      Value v = tuple.GetValue(plan->OutputSchema(), 0);
      results.push_back(v.CastAsDouble());
    }

    assert(results.size() == 3);
    // 10*5=50, 20*3=60, 15*8=120
    assert(results[0] == 50.0 && "10 * 5 = 50");
    assert(results[1] == 60.0 && "20 * 3 = 60");
    assert(results[2] == 120.0 && "15 * 8 = 120");
    std::cout << "[PASS] SELECT price * qty FROM products => [50, 60, 120]"
              << std::endl;
    std::cout << "\n=== ALL Bug 20 Tests PASSED ===" << std::endl;
    std::cout.flush();
    
    txn_mgr->Commit(txn);
    return 0;
  }
}
