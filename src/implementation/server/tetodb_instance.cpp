// tetodb_instance.cpp

#include "server/tetodb_instance.h"
#include <iostream>
#include <sstream>

// Frontend & Backend Headers
#include "execution/execution_engine.h"
#include "optimizer/optimizer.h"
#include "parser/lexer.h"
#include "parser/parser.h"
#include "planner/planner.h"
#include "recovery/recovery_manager.h"

namespace tetodb {

// Helper function to recursively build the EXPLAIN output string
void PrintPlanTree(const AbstractPlanNode *plan, std::ostringstream &oss,
                   int indent_level = 0) {
  if (plan == nullptr)
    return;

  std::string indent(indent_level * 2, ' ');
  oss << indent << "-> " << plan->ToString() << "\n";

  for (const auto *child : plan->GetChildren()) {
    PrintPlanTree(child, oss, indent_level + 1);
  }
}

TetoDBInstance::TetoDBInstance(const std::string &db_file_name) {
  disk_manager_ = std::make_unique<DiskManager>(db_file_name);
  replacer_ = std::make_unique<TwoQueueReplacer>(50);
  bpm_ = std::make_unique<BufferPoolManager>(50, disk_manager_.get(),
                                             replacer_.get());
  log_mgr_ = std::make_unique<LogManager>(disk_manager_.get());

  // ARIES Recovery
  RecoveryManager recovery_mgr(disk_manager_.get(), bpm_.get(), db_file_name);
  recovery_mgr.Redo();
  recovery_mgr.Undo();

  log_mgr_->RunFlushThread();
  lock_mgr_ = std::make_unique<LockManager>();
  txn_mgr_ =
      std::make_unique<TransactionManager>(lock_mgr_.get(), log_mgr_.get());
  std::string catalog_path =
      db_file_name.substr(0, db_file_name.find_last_of('.')) + ".catalog";
  catalog_ =
      std::make_unique<Catalog>(catalog_path, bpm_.get(), log_mgr_.get());

  checkpoint_mgr_ = std::make_unique<CheckpointManager>(
      txn_mgr_.get(), log_mgr_.get(), bpm_.get());
  checkpoint_mgr_->StartCheckpointer(std::chrono::seconds(5));

  catalog_->LoadCatalog(catalog_path);
  std::cout << "[SYSTEM] TetoDB Instance Online. Awaiting connections.\n";
}

TetoDBInstance::~TetoDBInstance() {
  std::cout << "[SYSTEM] Shutting down TetoDB Instance...\n";
  checkpoint_mgr_->StopCheckpointer();
  bpm_->FlushAllPages();
  log_mgr_->StopFlushThread();
}

QueryResult TetoDBInstance::ExecuteQuery(const std::string &sql,
                                         ClientSession &session) {
  QueryResult res;

  // =========================================================
  // ORM COMPATIBILITY INTERCEPT
  // Bypasses the Parser for standard PostgreSQL metadata checks
  // =========================================================
  std::string lower_sql = sql;
  std::transform(lower_sql.begin(), lower_sql.end(), lower_sql.begin(),
                 ::tolower);

  if (lower_sql.find("select pg_catalog.version()") != std::string::npos ||
      lower_sql.find("select version()") != std::string::npos) {
    res.owned_schema = std::make_shared<Schema>(
        std::vector<Column>{Column("version", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.rows.push_back(Tuple(
        {Value(TypeId::VARCHAR, "PostgreSQL 14.0 on TetoDB")}, res.schema));
    res.status_msg = "SELECT 1";
    return res;
  }

  if (lower_sql.find("select current_schema()") != std::string::npos) {
    res.owned_schema = std::make_shared<Schema>(
        std::vector<Column>{Column("current_schema", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.rows.push_back(Tuple({Value(TypeId::VARCHAR, "public")}, res.schema));
    res.status_msg = "SELECT 1";
    return res;
  }

  if (lower_sql.find("show standard_conforming_strings") != std::string::npos) {
    res.owned_schema = std::make_shared<Schema>(std::vector<Column>{
        Column("standard_conforming_strings", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.rows.push_back(Tuple({Value(TypeId::VARCHAR, "on")}, res.schema));
    res.status_msg = "SHOW";
    return res;
  }

  // Generic fallback for any other SHOW configuration commands
  if (lower_sql.rfind("show ", 0) == 0) {
    res.owned_schema = std::make_shared<Schema>(
        std::vector<Column>{Column("setting", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.rows.push_back(Tuple({Value(TypeId::VARCHAR, "on")}, res.schema));
    res.status_msg = "SHOW";
    return res;
  }

  // Generic fallback for any SET configuration commands
  if (lower_sql.rfind("set ", 0) == 0) {
    res.status_msg = "SET";
    return res;
  }

  // --- NEW: Intercept SQLAlchemy's String Encoding Pings ---
  if (lower_sql.find("cast('test plain returns'") != std::string::npos ||
      lower_sql.find("cast('test unicode returns'") != std::string::npos) {
    res.owned_schema = std::make_shared<Schema>(
        std::vector<Column>{Column("anon_1", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.rows.push_back(
        Tuple({Value(TypeId::VARCHAR, "test plain returns")}, res.schema));
    res.status_msg = "SELECT 1";
    return res;
  }

  // --- PostgreSQL System Catalog Queries ---
  if (lower_sql.find("pg_type") != std::string::npos ||
      lower_sql.find("pg_range") != std::string::npos ||
      lower_sql.find("pg_catalog") != std::string::npos ||
      lower_sql.find("pg_attribute") != std::string::npos ||
      lower_sql.find("information_schema") != std::string::npos) {
    res.owned_schema = std::make_shared<Schema>(
        std::vector<Column>{Column("name", TypeId::VARCHAR)});
    res.schema = res.owned_schema.get();
    res.status_msg = "SELECT 0";
    return res;
  }
  // =========================================================

  bool is_autocommit = (session.active_txn == nullptr);
  Transaction *exec_txn = nullptr;

  try {

    // --- Pass sql to the Lexer ---
    Lexer lexer(sql);
    auto tokens = lexer.TokenizeAll();

    if (tokens.empty() || tokens[0].type_ == TokenType::END_OF_FILE)
      return res;

    Parser parser(tokens);
    auto ast = parser.ParseStatement();

    // 1. Transaction Handling
    if (ast->type_ == ASTNodeType::TRANSACTION_STATEMENT) {
      auto *txn_stmt = static_cast<TransactionStatement *>(ast.get());
      if (txn_stmt->cmd_ == TransactionCmd::BEGIN) {
        if (session.active_txn)
          throw std::runtime_error("Transaction already in progress");
        session.active_txn = txn_mgr_->Begin();
        session.is_poisoned = false;
        res.status_msg = "BEGIN";
      } else if (txn_stmt->cmd_ == TransactionCmd::COMMIT) {
        if (!session.active_txn) {
          res.status_msg = "COMMIT";
        } else {
          if (session.is_poisoned) {
            txn_mgr_->Abort(session.active_txn);
            res.status_msg = "ROLLBACK";
          } else {
            txn_mgr_->Commit(session.active_txn);
            res.status_msg = "COMMIT";
          }
        }
        session.active_txn = nullptr;
        session.is_poisoned = false;
      } else if (txn_stmt->cmd_ == TransactionCmd::ROLLBACK) {
        if (session.active_txn)
          txn_mgr_->Abort(session.active_txn);
        session.active_txn = nullptr;
        session.is_poisoned = false;
        res.status_msg = "ROLLBACK";
      }
      return res;
    }

    if (ast->type_ == ASTNodeType::SET_STATEMENT) {
      res.status_msg = "SET";
      return res;
    }
    if (ast->type_ == ASTNodeType::SHOW_STATEMENT) {
      res.status_msg = "SHOW";
      return res;
    }

    // Savepoints: acknowledged as no-ops (no nested transaction support yet)
    if (ast->type_ == ASTNodeType::SAVEPOINT_STATEMENT) {
      auto *sp_stmt = static_cast<SavepointStatement *>(ast.get());
      switch (sp_stmt->cmd_) {
      case SavepointCmd::SAVEPOINT:
        res.status_msg = "SAVEPOINT";
        break;
      case SavepointCmd::RELEASE:
        res.status_msg = "RELEASE SAVEPOINT";
        break;
      case SavepointCmd::ROLLBACK_TO:
        res.status_msg = "ROLLBACK";
        break;
      }
      return res;
    }

    // Deallocate: acknowledged as no-op (prepared statements live in
    // ClientSession)
    if (ast->type_ == ASTNodeType::DEALLOCATE_STATEMENT) {
      res.status_msg = "DEALLOCATE";
      return res;
    }

    if (session.is_poisoned)
      throw std::runtime_error(
          "current transaction is aborted, commands ignored until end "
          "of transaction block");

    exec_txn = is_autocommit ? txn_mgr_->Begin() : session.active_txn;
    std::shared_lock<std::shared_mutex> cp_lock(txn_mgr_->GetGlobalTxnLatch());

    // 2. DDL Logic
    if (ast->type_ == ASTNodeType::CREATE_TABLE_STATEMENT) {
      std::unique_lock<std::shared_mutex> ddl_lock(ddl_latch_);
      auto *create_stmt = static_cast<CreateTableStatement *>(ast.get());
      std::vector<Column> cols;
      std::vector<uint32_t> pk_cols;
      uint32_t col_idx = 0;
      static const std::unordered_map<std::string, TypeId> type_map = {
          {"INT", TypeId::INTEGER},     {"INTEGER", TypeId::INTEGER},
          {"BIGINT", TypeId::BIGINT},   {"SMALLINT", TypeId::SMALLINT},
          {"TINYINT", TypeId::TINYINT}, {"BOOLEAN", TypeId::BOOLEAN},
          {"BOOL", TypeId::BOOLEAN},    {"DECIMAL", TypeId::DECIMAL},
          {"FLOAT", TypeId::DECIMAL},   {"DOUBLE", TypeId::DECIMAL},
          {"DATE", TypeId::TIMESTAMP},  {"TIMESTAMP", TypeId::TIMESTAMP},
          {"VARCHAR", TypeId::VARCHAR}, {"TEXT", TypeId::VARCHAR},
          {"CHAR", TypeId::CHAR},
      };
      for (const auto &c : create_stmt->columns_) {
        auto type_it = type_map.find(c.type_);
        TypeId tid =
            (type_it != type_map.end()) ? type_it->second : TypeId::VARCHAR;
        Column col(c.name_, tid);
        if (c.is_primary_key_) {
          pk_cols.push_back(col_idx);
        }
        col.SetNullable(!c.is_not_null_ && !c.is_primary_key_);
        cols.push_back(col);
        col_idx++;
      }
      if (catalog_->CreateTable(create_stmt->table_name_, Schema(cols),
                                INVALID_PAGE_ID, pk_cols,
                                create_stmt->foreign_keys_)) {
        res.status_msg = "CREATE TABLE";
      } else
        throw std::runtime_error("Table already exists");
    } else if (ast->type_ == ASTNodeType::CREATE_INDEX_STATEMENT) {
      std::unique_lock<std::shared_mutex> ddl_lock(ddl_latch_);
      auto *c_idx = static_cast<CreateIndexStatement *>(ast.get());
      if (catalog_->CreateIndex(c_idx->index_name_, c_idx->table_name_,
                                c_idx->index_columns_, c_idx->is_unique_,
                                exec_txn)) {
        res.status_msg = "CREATE INDEX";
      } else
        throw std::runtime_error("Index creation failed");
    } else if (ast->type_ == ASTNodeType::DROP_TABLE_STATEMENT) {
      std::unique_lock<std::shared_mutex> ddl_lock(ddl_latch_);
      auto *d_stmt = static_cast<DropTableStatement *>(ast.get());
      if (catalog_->DropTable(d_stmt->table_name_))
        res.status_msg = "DROP TABLE";
      else
        throw std::runtime_error("Table not found");
    }
    // 3. DML/Query Logic
    else {
      std::shared_lock<std::shared_mutex> dml_lock(ddl_latch_);
      ExecutionContext exec_ctx(catalog_.get(), bpm_.get(), exec_txn,
                                lock_mgr_.get(), txn_mgr_.get(),
                                &session.current_parameters);
      Planner planner(catalog_.get(), &exec_ctx);

      const ASTNode *target_ast = ast.get();
      if (ast->type_ == ASTNodeType::EXPLAIN_STATEMENT) {
        target_ast =
            static_cast<ExplainStatement *>(ast.get())->inner_statement_.get();
      }

      const AbstractPlanNode *logical_plan = planner.PlanQuery(target_ast);

      Optimizer optimizer(catalog_.get());
      const AbstractPlanNode *physical_plan = optimizer.Optimize(logical_plan);

      auto root_executor =
          ExecutionEngine::CreateExecutor(physical_plan, &exec_ctx);
      root_executor->Init();

      if (root_executor->GetOutputSchema()) {
        res.owned_schema =
            std::make_shared<Schema>(*root_executor->GetOutputSchema());
        res.schema = res.owned_schema.get();
      }
      Tuple tuple;
      RID rid;
      while (root_executor->Next(&tuple, &rid)) {
        res.rows.push_back(tuple);
      }

      PlanType root_type = physical_plan->GetPlanType();
      if (root_type == PlanType::Insert)
        res.status_msg = "INSERT 0 " + std::to_string(res.rows.size());
      else if (root_type == PlanType::Update)
        res.status_msg = "UPDATE " + std::to_string(res.rows.size());
      else if (root_type == PlanType::Delete)
        res.status_msg = "DELETE " + std::to_string(res.rows.size());
      else
        res.status_msg = "SELECT " + std::to_string(res.rows.size());
    }

    if (is_autocommit)
      txn_mgr_->Commit(exec_txn);
  } catch (const std::exception &e) {
    if (exec_txn && is_autocommit)
      txn_mgr_->Abort(exec_txn);
    if (!is_autocommit)
      session.is_poisoned = true;
    res.is_error = true;
    res.status_msg = "[!] Error: " + std::string(e.what());
  }
  return res;
}
} // namespace tetodb