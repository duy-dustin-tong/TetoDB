// parser.cpp

#include "parser/parser.h"
#include <algorithm> // transform

namespace tetodb {

Parser::Parser(const std::vector<Token> &tokens) : tokens_(tokens) {}

// ==========================================
// MASTER ROUTER
// ==========================================
std::unique_ptr<ASTNode> Parser::ParseStatement() {

  if (Peek().value_ == "EXPLAIN") {
    Advance();                          // Consume 'EXPLAIN' token
    auto inner_stmt = ParseStatement(); // Recursively parse the actual query
    if (!inner_stmt) {
      throw std::runtime_error(
          "Syntax Error: Expected valid SQL statement after EXPLAIN");
    }
    return std::make_unique<ExplainStatement>(std::move(inner_stmt));
  }

  if (Peek().value_ == "SELECT")
    return ParseSelect();
  if (Peek().value_ == "INSERT")
    return ParseInsert();
  if (Peek().value_ == "UPDATE")
    return ParseUpdate();
  if (Peek().value_ == "DELETE")
    return ParseDelete();

  if (Peek().value_ == "CREATE") {
    if (Peek(1).value_ == "TABLE")
      return ParseCreateTable();
    // --- NEW: Check for either CREATE INDEX or CREATE UNIQUE INDEX ---
    if (Peek(1).value_ == "INDEX" ||
        (Peek(1).value_ == "UNIQUE" && Peek(2).value_ == "INDEX"))
      return ParseCreateIndex();
    throw std::runtime_error("Syntax Error: Unknown CREATE statement type");
  }
  if (Peek().value_ == "DROP") {
    if (Peek(1).value_ == "TABLE")
      return ParseDropTable();
    throw std::runtime_error("Syntax Error: Unknown DROP statement type");
  }

  if (Peek().value_ == "BEGIN") {
    Advance();
    return std::make_unique<TransactionStatement>(TransactionCmd::BEGIN);
  }
  if (Peek().value_ == "COMMIT") {
    Advance();
    return std::make_unique<TransactionStatement>(TransactionCmd::COMMIT);
  }

  if (Peek().value_ == "ROLLBACK") {
    Advance();
    // ROLLBACK TO [SAVEPOINT] <name>
    if (Match(TokenType::KEYWORD, "TO")) {
      Match(TokenType::KEYWORD, "SAVEPOINT"); // optional keyword
      // Accept both IDENTIFIER and STRING (psycopg3 sends quoted names like
      // "_pg3_1")
      if (Peek().type_ != TokenType::IDENTIFIER &&
          Peek().type_ != TokenType::STRING) {
        throw std::runtime_error("Expected savepoint name after ROLLBACK TO");
      }
      std::string sp_name = Advance().value_;
      return std::make_unique<SavepointStatement>(SavepointCmd::ROLLBACK_TO,
                                                  sp_name);
    }
    return std::make_unique<TransactionStatement>(TransactionCmd::ROLLBACK);
  }

  if (Peek().value_ == "SAVEPOINT") {
    Advance();
    // Accept both IDENTIFIER and STRING for savepoint name
    if (Peek().type_ != TokenType::IDENTIFIER &&
        Peek().type_ != TokenType::STRING) {
      throw std::runtime_error("Expected savepoint name after SAVEPOINT");
    }
    std::string sp_name = Advance().value_;
    return std::make_unique<SavepointStatement>(SavepointCmd::SAVEPOINT,
                                                sp_name);
  }

  if (Peek().value_ == "RELEASE") {
    Advance();
    Match(TokenType::KEYWORD, "SAVEPOINT"); // optional keyword
    // Accept both IDENTIFIER and STRING for savepoint name
    if (Peek().type_ != TokenType::IDENTIFIER &&
        Peek().type_ != TokenType::STRING) {
      throw std::runtime_error("Expected savepoint name after RELEASE");
    }
    std::string sp_name = Advance().value_;
    return std::make_unique<SavepointStatement>(SavepointCmd::RELEASE, sp_name);
  }

  if (Peek().value_ == "DEALLOCATE") {
    Advance();
    // DEALLOCATE ALL or DEALLOCATE <name>
    if (Peek().type_ == TokenType::IDENTIFIER ||
        Peek().type_ == TokenType::STRING || Peek().value_ == "ALL") {
      std::string name = Advance().value_;
      return std::make_unique<DeallocateStatement>(name);
    }
    return std::make_unique<DeallocateStatement>("ALL");
  }

  throw std::runtime_error("Syntax Error: Unknown statement type '" +
                           Peek().value_ + "'");
}

// ==========================================
// MUTATION PARSERS
// ==========================================
std::unique_ptr<InsertStatement> Parser::ParseInsert() {
  auto stmt = std::make_unique<InsertStatement>();
  Consume(TokenType::KEYWORD, "Expected INSERT");
  Consume(TokenType::KEYWORD, "Expected INTO");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  stmt->table_name_ = tokens_[cursor_ - 1].value_;

  Consume(TokenType::KEYWORD, "Expected VALUES");

  do {
    Consume(TokenType::SYMBOL, "Expected '(' before values");
    std::vector<std::unique_ptr<Expr>> row;
    do {
      row.push_back(ParseExpression());
    } while (Match(TokenType::SYMBOL, ","));
    Consume(TokenType::SYMBOL, "Expected ')' after values");

    stmt->values_.push_back(std::move(row));
  } while (Match(TokenType::SYMBOL, ","));

  return stmt;
}

std::unique_ptr<UpdateStatement> Parser::ParseUpdate() {
  auto stmt = std::make_unique<UpdateStatement>();
  Consume(TokenType::KEYWORD, "Expected UPDATE");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  stmt->table_name_ = tokens_[cursor_ - 1].value_;

  Consume(TokenType::KEYWORD, "Expected SET");

  do {
    Consume(TokenType::IDENTIFIER, "Expected column name");
    std::string col_name = tokens_[cursor_ - 1].value_;
    Consume(TokenType::SYMBOL, "Expected '='");
    auto val_expr = ParseExpression();
    stmt->set_clauses_.push_back({col_name, std::move(val_expr)});
  } while (Match(TokenType::SYMBOL, ","));

  if (Match(TokenType::KEYWORD, "WHERE")) {
    stmt->where_clause_ = ParseExpression();
  }

  return stmt;
}

std::unique_ptr<DeleteStatement> Parser::ParseDelete() {
  auto stmt = std::make_unique<DeleteStatement>();
  Consume(TokenType::KEYWORD, "Expected DELETE");
  Consume(TokenType::KEYWORD, "Expected FROM");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  stmt->table_name_ = tokens_[cursor_ - 1].value_;

  if (Match(TokenType::KEYWORD, "WHERE")) {
    stmt->where_clause_ = ParseExpression();
  }

  return stmt;
}

std::unique_ptr<CreateTableStatement> Parser::ParseCreateTable() {
  auto stmt = std::make_unique<CreateTableStatement>();

  Consume(TokenType::KEYWORD, "Expected CREATE");
  Consume(TokenType::KEYWORD, "Expected TABLE");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  stmt->table_name_ = tokens_[cursor_ - 1].value_;

  Consume(TokenType::SYMBOL, "Expected '(' to start table definitions");

  do {
    // --- BRANCH 1: FOREIGN KEY CONSTRAINT ---
    if (Match(TokenType::KEYWORD, "FOREIGN")) {
      if (!Match(TokenType::KEYWORD, "KEY"))
        throw std::runtime_error(
            "Syntax Error: Expected 'KEY' after 'FOREIGN'");

      Consume(TokenType::SYMBOL, "Expected '('");
      Consume(TokenType::IDENTIFIER, "Expected child column");
      std::string child_col = tokens_[cursor_ - 1].value_;
      Consume(TokenType::SYMBOL, "Expected ')'");

      if (!Match(TokenType::KEYWORD, "REFERENCES"))
        throw std::runtime_error("Syntax Error: Expected 'REFERENCES'");

      Consume(TokenType::IDENTIFIER, "Expected parent table");
      std::string parent_table = tokens_[cursor_ - 1].value_;

      Consume(TokenType::SYMBOL, "Expected '('");
      Consume(TokenType::IDENTIFIER, "Expected parent column");
      std::string parent_col = tokens_[cursor_ - 1].value_;
      Consume(TokenType::SYMBOL, "Expected ')'");

      ReferentialAction on_delete = ReferentialAction::RESTRICT;
      ReferentialAction on_update = ReferentialAction::RESTRICT;

      while (Match(TokenType::KEYWORD, "ON")) {
        if (Match(TokenType::KEYWORD, "DELETE")) {
          if (Match(TokenType::KEYWORD, "CASCADE"))
            on_delete = ReferentialAction::CASCADE;
          else if (Match(TokenType::KEYWORD, "SET") &&
                   Match(TokenType::KEYWORD, "NULL"))
            on_delete = ReferentialAction::SET_NULL;
          else if (Match(TokenType::KEYWORD, "RESTRICT"))
            on_delete = ReferentialAction::RESTRICT;
          else
            throw std::runtime_error("Syntax Error: Invalid ON DELETE action");
        } else if (Match(TokenType::KEYWORD, "UPDATE")) {
          if (Match(TokenType::KEYWORD, "CASCADE"))
            on_update = ReferentialAction::CASCADE;
          else if (Match(TokenType::KEYWORD, "SET") &&
                   Match(TokenType::KEYWORD, "NULL"))
            on_update = ReferentialAction::SET_NULL;
          else if (Match(TokenType::KEYWORD, "RESTRICT"))
            on_update = ReferentialAction::RESTRICT;
          else
            throw std::runtime_error("Syntax Error: Invalid ON UPDATE action");
        } else {
          throw std::runtime_error(
              "Syntax Error: Expected 'DELETE' or 'UPDATE' after 'ON'");
        }
      }
      stmt->foreign_keys_.push_back(
          {child_col, parent_table, parent_col, on_delete, on_update});
    }
    // --- BRANCH 2: COLUMN DEFINITION ---
    else {
      Consume(TokenType::IDENTIFIER, "Expected column name");
      std::string col_name = tokens_[cursor_ - 1].value_;

      Consume(TokenType::KEYWORD,
              "Expected column data type (e.g., INT, VARCHAR)");
      std::string col_type = tokens_[cursor_ - 1].value_;

      ColumnDef col(col_name, col_type);

      if (Match(TokenType::KEYWORD, "PRIMARY")) {
        if (Match(TokenType::KEYWORD, "KEY")) {
          col.is_primary_key_ = true;
          col.is_not_null_ = true; // PK is implicitly NOT NULL
        } else {
          throw std::runtime_error(
              "Syntax Error: Expected 'KEY' after 'PRIMARY'");
        }
      }

      // Parse NOT NULL constraint (can appear after PRIMARY KEY or standalone)
      if (Match(TokenType::KEYWORD, "NOT")) {
        if (Match(TokenType::KEYWORD, "NULL")) {
          col.is_not_null_ = true;
        } else {
          throw std::runtime_error("Syntax Error: Expected 'NULL' after 'NOT'");
        }
      }

      stmt->columns_.push_back(col);
    }
  } while (Match(TokenType::SYMBOL, ","));

  Consume(TokenType::SYMBOL, "Expected ')' to end table definitions");
  return stmt;
}

std::unique_ptr<DropTableStatement> Parser::ParseDropTable() {
  Consume(TokenType::KEYWORD, "Expected DROP");
  Consume(TokenType::KEYWORD, "Expected TABLE");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  std::string table_name = tokens_[cursor_ - 1].value_;

  // Optional semicolon at the end
  if (Match(TokenType::SYMBOL, ";")) {
    // Semicolon consumed
  }

  return std::make_unique<DropTableStatement>(table_name);
}

std::unique_ptr<CreateIndexStatement> Parser::ParseCreateIndex() {
  auto stmt = std::make_unique<CreateIndexStatement>();

  Consume(TokenType::KEYWORD, "Expected CREATE");

  if (Match(TokenType::KEYWORD, "UNIQUE")) {
    stmt->is_unique_ = true;
  }

  Consume(TokenType::KEYWORD, "Expected INDEX");

  Consume(TokenType::IDENTIFIER, "Expected index name");
  stmt->index_name_ = tokens_[cursor_ - 1].value_;

  Consume(TokenType::KEYWORD, "Expected ON");

  Consume(TokenType::IDENTIFIER, "Expected table name");
  stmt->table_name_ = tokens_[cursor_ - 1].value_;

  Consume(TokenType::SYMBOL, "Expected '(' to start index columns");

  do {
    Consume(TokenType::IDENTIFIER, "Expected column name");
    stmt->index_columns_.push_back(tokens_[cursor_ - 1].value_);
  } while (Match(TokenType::SYMBOL, ","));

  Consume(TokenType::SYMBOL, "Expected ')' to end index columns");

  return stmt;
}

std::unique_ptr<SelectStatement> Parser::ParseSelect() {
  auto stmt = std::make_unique<SelectStatement>();

  Consume(TokenType::KEYWORD, "Expected SELECT");
  if (tokens_[cursor_ - 1].value_ != "SELECT") {
    throw std::runtime_error("Syntax Error: Query must start with SELECT");
  }

  do {
    if (Match(TokenType::SYMBOL, "*")) {
      stmt->select_list_.push_back(std::make_unique<ColumnRefExpr>("", "*"));
    } else {
      auto expr = ParseExpression();

      if (Match(TokenType::KEYWORD, "AS")) {
        Consume(TokenType::IDENTIFIER, "Expected column alias after AS");
        expr->alias_ = tokens_[cursor_ - 1].value_;
      } else if (Peek().type_ == TokenType::IDENTIFIER) {
        expr->alias_ = Advance().value_;
      }

      stmt->select_list_.push_back(std::move(expr));
    }
  } while (Match(TokenType::SYMBOL, ","));

  if (Match(TokenType::KEYWORD, "FROM")) {
    stmt->from_table_ = ParseTableRef();
  } else {
    throw std::runtime_error("Syntax Error: Expected FROM clause");
  }

  while (Match(TokenType::KEYWORD, "JOIN")) {
    stmt->joins_.push_back(ParseJoin());
  }

  if (Match(TokenType::KEYWORD, "WHERE")) {
    stmt->where_clause_ = ParseExpression();
  }

  if (Match(TokenType::KEYWORD, "GROUP")) {
    if (!Match(TokenType::KEYWORD, "BY")) {
      throw std::runtime_error("Syntax Error: Expected 'BY' after 'GROUP'");
    }
    do {
      stmt->group_bys_.push_back(ParseExpression());
    } while (Match(TokenType::SYMBOL, ","));
  }

  if (Match(TokenType::KEYWORD, "ORDER")) {
    Consume(TokenType::KEYWORD, "Expected 'BY' after 'ORDER'");

    do {
      auto expr = ParseExpression();
      bool is_desc = false;

      if (Match(TokenType::KEYWORD, "DESC")) {
        is_desc = true;
      } else if (Match(TokenType::KEYWORD, "ASC")) {
        is_desc = false;
      }

      stmt->order_bys_.push_back(
          std::make_unique<OrderByNode>(std::move(expr), is_desc));

    } while (Match(TokenType::SYMBOL, ","));
  }

  if (Match(TokenType::KEYWORD, "LIMIT")) {
    Consume(TokenType::NUMBER, "Expected number after LIMIT");
    stmt->limit_count_ = std::stoi(tokens_[cursor_ - 1].value_);
  }

  if (Match(TokenType::KEYWORD, "OFFSET")) {
    Consume(TokenType::NUMBER, "Expected number after OFFSET");
    stmt->offset_count_ = std::stoi(tokens_[cursor_ - 1].value_);
  }

  return stmt;
}

std::unique_ptr<Expr> Parser::ParseExpression() { return ParseOrExpression(); }

std::unique_ptr<Expr> Parser::ParseOrExpression() {
  auto left = ParseAndExpression();
  while (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "OR") {
    std::string op = Advance().value_;
    auto right = ParseAndExpression();
    left = std::make_unique<LogicalExpr>(std::move(left), op, std::move(right));
  }
  return left;
}

std::unique_ptr<Expr> Parser::ParseAndExpression() {
  auto left = ParseNotExpression();
  while (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "AND") {
    std::string op = Advance().value_;
    auto right = ParseNotExpression();
    left = std::make_unique<LogicalExpr>(std::move(left), op, std::move(right));
  }
  return left;
}

std::unique_ptr<Expr> Parser::ParseNotExpression() {
  if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "NOT") {
    Advance();
    auto child =
        ParseComparisonExpression(); // Because 'NOT a = b' is invalid in
                                     // standard SQL, but PostgreSQL supports
                                     // 'NOT (a = b)' via parenthesis. Wait,
                                     // standard SQL allows NOT IN and NOT
                                     // BETWEEN. We will handle NOT IN inside
                                     // ParseComparisonExpression. This leading
                                     // NOT is purely unary prefix, like NOT
                                     // valid.
    return std::make_unique<NotExpr>(std::move(child));
  }
  return ParseComparisonExpression();
}

std::unique_ptr<Expr> Parser::ParseComparisonExpression() {
  auto left = ParseBaseExpression();

  if (Peek().type_ == TokenType::SYMBOL &&
      (Peek().value_ == "=" || Peek().value_ == ">" || Peek().value_ == "<" ||
       Peek().value_ == ">=" || Peek().value_ == "<=" ||
       Peek().value_ == "!=")) {

    std::string op = Advance().value_;
    auto right = ParseBaseExpression();
    left = std::make_unique<BinaryExpr>(std::move(left), op, std::move(right));
  }

  if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "IS") {
    Advance();
    std::string op = "IS_NULL";
    if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "NOT") {
      Advance();
      op = "IS_NOT_NULL";
    }
    if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "NULL") {
      Advance();
      left = std::make_unique<BinaryExpr>(
          std::move(left), op, std::make_unique<ConstantExpr>("NULL"));
    } else {
      throw std::runtime_error("Syntax Error: Expected 'NULL' after 'IS'");
    }
  }

  // Handle potential trailing [NOT] IN / BETWEEN
  bool is_not = false;
  if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "NOT") {
    is_not = true;
    Advance();
  }

  if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "IN") {
    Advance();
    Consume(TokenType::SYMBOL, "(");
    std::vector<std::unique_ptr<Expr>> in_list;
    do {
      in_list.push_back(ParseExpression());
    } while (Match(TokenType::SYMBOL, ","));
    Consume(TokenType::SYMBOL, ")");
    return std::make_unique<InExpr>(std::move(left), std::move(in_list),
                                    is_not);
  }

  if (Peek().type_ == TokenType::KEYWORD && Peek().value_ == "BETWEEN") {
    Advance();
    auto lower = ParseBaseExpression();
    Consume(TokenType::KEYWORD, "AND");
    auto upper = ParseBaseExpression();
    return std::make_unique<BetweenExpr>(std::move(left), std::move(lower),
                                         std::move(upper), is_not);
  }

  if (is_not) {
    throw std::runtime_error("Syntax Error: Unexpected NOT after expression "
                             "(expected IN or BETWEEN)");
  }

  return left;
}

std::unique_ptr<Expr> Parser::ParseBaseExpression() {
  if (Match(TokenType::SYMBOL, "(")) {
    auto expr = ParseExpression();
    Consume(TokenType::SYMBOL, ")");
    return expr;
  }

  if (Match(TokenType::KEYWORD, "NULL")) {
    return std::make_unique<ConstantExpr>("NULL");
  }

  if (Match(TokenType::KEYWORD, "TRUE")) {
    return std::make_unique<ConstantExpr>("TRUE");
  }
  if (Match(TokenType::KEYWORD, "FALSE")) {
    return std::make_unique<ConstantExpr>("FALSE");
  }

  if (Match(TokenType::NUMBER)) {
    return std::make_unique<ConstantExpr>(tokens_[cursor_ - 1].value_);
  }

  if (Match(TokenType::STRING)) {
    return std::make_unique<ConstantExpr>(tokens_[cursor_ - 1].value_);
  }

  if (Match(TokenType::PARAMETER)) {
    uint32_t p_idx = std::stoul(tokens_[cursor_ - 1].value_);
    return std::make_unique<ParameterExpr>(p_idx);
  }

  std::string upper_val = Peek().value_;
  std::transform(upper_val.begin(), upper_val.end(), upper_val.begin(),
                 ::toupper);

  if (Peek().type_ == TokenType::KEYWORD &&
      (upper_val == "COUNT" || upper_val == "SUM" || upper_val == "MIN" ||
       upper_val == "MAX" || upper_val == "AVG" || upper_val == "AVERAGE" ||
       upper_val == "MED" || upper_val == "MEDIAN")) {

    Advance();

    if (!Match(TokenType::SYMBOL, "(")) {
      throw std::runtime_error(
          "Syntax Error: Expected '(' after aggregate function");
    }
    auto arg = ParseExpression();
    if (!Match(TokenType::SYMBOL, ")")) {
      throw std::runtime_error(
          "Syntax Error: Expected ')' after aggregate argument");
    }

    return std::make_unique<AggregateExpr>(upper_val, std::move(arg));
  }

  if (Match(TokenType::IDENTIFIER)) {
    std::string part1 = tokens_[cursor_ - 1].value_;

    if (Match(TokenType::SYMBOL, ".")) {
      Consume(TokenType::IDENTIFIER, "Expected column name after dot");
      std::string part2 = tokens_[cursor_ - 1].value_;
      return std::make_unique<ColumnRefExpr>(part1, part2);
    }

    return std::make_unique<ColumnRefExpr>("", part1);
  }

  throw std::runtime_error("Syntax Error: Expected expression (Column, Number, "
                           "String, or Aggregate)");
}

std::unique_ptr<TableRef> Parser::ParseTableRef() {
  Consume(TokenType::IDENTIFIER, "Expected table name");
  std::string table_name = tokens_[cursor_ - 1].value_;
  std::string alias = "";

  if (Match(TokenType::KEYWORD, "AS")) {
    Consume(TokenType::IDENTIFIER, "Expected alias after AS");
    alias = tokens_[cursor_ - 1].value_;
  } else if (Peek().type_ == TokenType::IDENTIFIER) {
    alias = Advance().value_;
  }

  return std::make_unique<TableRef>(table_name, alias);
}

std::unique_ptr<JoinNode> Parser::ParseJoin() {
  auto right_table = ParseTableRef();
  if (!Match(TokenType::KEYWORD, "ON")) {
    throw std::runtime_error("Syntax Error: Expected 'ON' after JOIN table");
  }
  auto condition = ParseExpression();
  return std::make_unique<JoinNode>(std::move(right_table),
                                    std::move(condition));
}

// --- Helpers ---
const Token &Parser::Peek(int offset) const {
  if (cursor_ + offset >= tokens_.size())
    return tokens_.back();
  return tokens_[cursor_ + offset];
}

const Token &Parser::Advance() {
  if (cursor_ < tokens_.size())
    cursor_++;
  return tokens_[cursor_ - 1];
}

bool Parser::Match(TokenType type, const std::string &value) {
  if (Peek().type_ == type && (value.empty() || Peek().value_ == value)) {
    Advance();
    return true;
  }
  return false;
}

void Parser::Consume(TokenType type, const std::string &error_msg) {
  if (Peek().type_ == type) {
    Advance();
    return;
  }
  throw std::runtime_error(error_msg + " but found '" + Peek().value_ + "'");
}

} // namespace tetodb