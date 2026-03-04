// parser.h

#pragma once

#include "index/index.h"
#include "parser/lexer.h"
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace tetodb {
// Full definitions are in parser/ast.h, which is included in parser.cpp
struct ASTNode;
struct SelectStatement;
struct InsertStatement;
struct UpdateStatement;
struct DeleteStatement;
struct Expr;
struct TableRef;
struct JoinNode;
struct CreateTableStatement;
struct CreateIndexStatement;
struct CreateViewStatement;
struct DropTableStatement;
struct DropIndexStatement;
struct DropViewStatement;

class Parser {
public:
  explicit Parser(const std::vector<Token> &tokens);

  std::unique_ptr<ASTNode> ParseStatement();

private:
  std::vector<Token> tokens_;
  size_t cursor_{0};

  // Parsers
  std::unique_ptr<SelectStatement> ParseSelect();
  std::unique_ptr<InsertStatement> ParseInsert();
  std::unique_ptr<UpdateStatement> ParseUpdate();
  std::unique_ptr<DeleteStatement> ParseDelete();

  // --- Core Grammar Rules ---
  std::unique_ptr<Expr> ParseExpression();
  std::unique_ptr<Expr> ParseOrExpression();
  std::unique_ptr<Expr> ParseAndExpression();
  std::unique_ptr<Expr> ParseNotExpression();
  std::unique_ptr<Expr> ParseComparisonExpression();
  std::unique_ptr<Expr> ParseBaseExpression(); // Handles columns and numbers
  std::unique_ptr<TableRef> ParseTableRef();
  std::unique_ptr<JoinNode> ParseJoin();
  std::unique_ptr<CreateTableStatement> ParseCreateTable();
  std::unique_ptr<CreateIndexStatement> ParseCreateIndex();
  std::unique_ptr<CreateViewStatement> ParseCreateView();
  std::unique_ptr<DropTableStatement> ParseDropTable(); // <-- ADDED
  std::unique_ptr<DropIndexStatement> ParseDropIndex();
  std::unique_ptr<DropViewStatement> ParseDropView();

  // --- Token Helpers ---
  const Token &Peek(int offset = 0) const;
  const Token &Advance();
  bool Match(TokenType type, const std::string &value = "");
  void Consume(TokenType type, const std::string &error_msg);
};

} // namespace tetodb