// ast.h

#pragma once

#include "parser/lexer.h"
#include <memory>
#include <string>
#include <vector>

namespace tetodb {

// ast.h (Add to ASTNodeType enum and add the struct)
enum class ASTNodeType {
  SELECT_STATEMENT,
  INSERT_STATEMENT,
  UPDATE_STATEMENT,
  DELETE_STATEMENT,
  CREATE_TABLE_STATEMENT,
  CREATE_INDEX_STATEMENT,
  DROP_TABLE_STATEMENT,
  DROP_INDEX_STATEMENT,
  TRANSACTION_STATEMENT,
  EXPLAIN_STATEMENT,
  SAVEPOINT_STATEMENT,
  DEALLOCATE_STATEMENT,
  SET_STATEMENT,
  SHOW_STATEMENT,
  COLUMN_REF,
  CONSTANT,
  BINARY_EXPR,
  LOGICAL_EXPR,
  NOT_EXPR,
  IN_EXPR,
  BETWEEN_EXPR,
  FUNCTION_EXPR,
  TABLE_REF,
  JOIN,
  ORDER_BY,
  AGGREGATE,
  SETOP_STATEMENT, // UNION / INTERSECT / EXCEPT
  PARAMETER_EXPR
};

enum class TransactionCmd { BEGIN, COMMIT, ROLLBACK };

enum class SavepointCmd { SAVEPOINT, RELEASE, ROLLBACK_TO };

enum class ReferentialAction {
  RESTRICT, // Default: Abort the transaction if child rows exist
  CASCADE,  // Automatically delete/update matching child rows
  SET_NULL  // Set child columns to NULL (if supported by schema)
};

struct SelectStatement;

// Base Tree Node
struct ASTNode {
  ASTNodeType type_;
  virtual ~ASTNode() = default;
  virtual std::string ToString(int indent = 0) const = 0;

  std::string Indent(int level) const { return std::string(level * 2, ' '); }
};

struct TransactionStatement : public ASTNode {
  TransactionCmd cmd_;

  TransactionStatement(TransactionCmd cmd) : cmd_(cmd) {
    type_ = ASTNodeType::TRANSACTION_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    std::string cmd_str = (cmd_ == TransactionCmd::BEGIN)    ? "BEGIN"
                          : (cmd_ == TransactionCmd::COMMIT) ? "COMMIT"
                                                             : "ROLLBACK";
    return Indent(indent) + "[[ TRANSACTION: " + cmd_str + " ]]\n";
  }
};

// Base Expression Node
struct Expr : public ASTNode {
  std::string alias_; // <--- NEW: Every expression can now have an alias!
};

struct ColumnRefExpr : public Expr {
  std::string table_alias_;
  std::string col_name_;

  ColumnRefExpr(std::string t, std::string c) : table_alias_(t), col_name_(c) {
    type_ = ASTNodeType::COLUMN_REF;
  }

  std::string ToString(int indent = 0) const override {
    std::string prefix = table_alias_.empty() ? "" : table_alias_ + ".";
    std::string alias_suffix =
        alias_.empty() ? "" : " AS " + alias_; // <--- Print it!
    return Indent(indent) + "[ColumnRef: " + prefix + col_name_ + alias_suffix +
           "]\n";
  }
};

struct ConstantExpr : public Expr {
  std::string value_;

  ConstantExpr(std::string v) : value_(v) { type_ = ASTNodeType::CONSTANT; }

  std::string ToString(int indent = 0) const override {
    std::string alias_suffix =
        alias_.empty() ? "" : " AS " + alias_; // <--- Print it!
    return Indent(indent) + "[Constant: " + value_ + alias_suffix + "]\n";
  }
};

// E.g., o.total >= 250
struct BinaryExpr : public Expr {
  std::unique_ptr<Expr> left_;
  std::string op_; // =, >, <, >=, <=, AND, OR
  std::unique_ptr<Expr> right_;

  BinaryExpr(std::unique_ptr<Expr> l, std::string op, std::unique_ptr<Expr> r)
      : left_(std::move(l)), op_(op), right_(std::move(r)) {
    type_ = ASTNodeType::BINARY_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[BinaryExpr: " + op_ + "]\n";
    res += left_->ToString(indent + 1);
    res += right_->ToString(indent + 1);
    return res;
  }
};

// E.g., a > 5 AND b < 10
struct LogicalExpr : public Expr {
  std::unique_ptr<Expr> left_;
  std::string op_; // AND, OR
  std::unique_ptr<Expr> right_;

  LogicalExpr(std::unique_ptr<Expr> l, std::string op, std::unique_ptr<Expr> r)
      : left_(std::move(l)), op_(op), right_(std::move(r)) {
    type_ = ASTNodeType::LOGICAL_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[LogicalExpr: " + op_ + "]\n";
    if (left_)
      res += left_->ToString(indent + 1);
    if (right_)
      res += right_->ToString(indent + 1);
    return res;
  }
};

// E.g., NOT (a > 5)
struct NotExpr : public Expr {
  std::unique_ptr<Expr> child_;

  NotExpr(std::unique_ptr<Expr> child) : child_(std::move(child)) {
    type_ = ASTNodeType::NOT_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    return Indent(indent) + "[NotExpr]\n" + child_->ToString(indent + 1);
  }
};

enum class SetOpType { UNION, INTERSECT, EXCEPT };

struct SetOpStatement : public ASTNode {
  SetOpType set_op_type_;
  bool is_all_;
  std::unique_ptr<ASTNode> left_;
  std::unique_ptr<ASTNode> right_;

  SetOpStatement(SetOpType op_type, bool is_all, std::unique_ptr<ASTNode> left,
                 std::unique_ptr<ASTNode> right)
      : set_op_type_(op_type), is_all_(is_all), left_(std::move(left)),
        right_(std::move(right)) {
    type_ = ASTNodeType::SETOP_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    std::string op_str;
    switch (set_op_type_) {
    case SetOpType::UNION:
      op_str = "UNION";
      break;
    case SetOpType::INTERSECT:
      op_str = "INTERSECT";
      break;
    case SetOpType::EXCEPT:
      op_str = "EXCEPT";
      break;
    }
    if (is_all_) {
      op_str += " ALL";
    }

    std::string res = Indent(indent) + "[[ SET OPERATION : " + op_str + " ]]\n";
    res += Indent(indent + 1) + "- LEFT:\n";
    res += left_->ToString(indent + 2);
    res += Indent(indent + 1) + "- RIGHT:\n";
    res += right_->ToString(indent + 2);
    return res;
  }
};

// E.g., id IN (1, 2, 3)
struct InExpr : public Expr {
  std::unique_ptr<Expr> left_;
  std::vector<std::unique_ptr<Expr>> in_list_;
  bool is_not_;
  std::unique_ptr<SelectStatement> subquery_;

  InExpr(std::unique_ptr<Expr> l, std::vector<std::unique_ptr<Expr>> list,
         bool is_not = false,
         std::unique_ptr<SelectStatement> subquery = nullptr)
      : left_(std::move(l)), in_list_(std::move(list)), is_not_(is_not),
        subquery_(std::move(subquery)) {
    type_ = ASTNodeType::IN_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    std::string res =
        Indent(indent) + "[InExpr" + (is_not_ ? " NOT" : "") + "]\n";
    res += left_->ToString(indent + 1);
    if (subquery_) {
      res += Indent(indent + 1) + "- IN SUBQUERY:\n";
      res += reinterpret_cast<const ASTNode *>(subquery_.get())
                 ->ToString(indent + 2);
    } else {
      res += Indent(indent + 1) + "- IN LIST:\n";
      for (const auto &expr : in_list_) {
        res += expr->ToString(indent + 2);
      }
    }
    return res;
  }
};

// E.g., age BETWEEN 18 AND 65
struct BetweenExpr : public Expr {
  std::unique_ptr<Expr> expr_;
  std::unique_ptr<Expr> lower_;
  std::unique_ptr<Expr> upper_;
  bool is_not_;

  BetweenExpr(std::unique_ptr<Expr> expr, std::unique_ptr<Expr> lower,
              std::unique_ptr<Expr> upper, bool is_not = false)
      : expr_(std::move(expr)), lower_(std::move(lower)),
        upper_(std::move(upper)), is_not_(is_not) {
    type_ = ASTNodeType::BETWEEN_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    std::string res =
        Indent(indent) + "[BetweenExpr" + (is_not_ ? " NOT" : "") + "]\n";
    res += expr_->ToString(indent + 1);
    res += Indent(indent + 1) + "- LOWER:\n" + lower_->ToString(indent + 2);
    res += Indent(indent + 1) + "- UPPER:\n" + upper_->ToString(indent + 2);
    return res;
  }
};

// E.g., users u
struct TableRef : public ASTNode {
  std::string table_name_;
  std::string alias_;
  std::unique_ptr<SelectStatement> subquery_;

  TableRef(std::string name, std::string alias,
           std::unique_ptr<SelectStatement> subquery = nullptr)
      : table_name_(name), alias_(alias), subquery_(std::move(subquery)) {
    type_ = ASTNodeType::TABLE_REF;
  }

  std::string ToString(int indent = 0) const override {
    std::string a = alias_.empty() ? "" : " AS " + alias_;
    if (subquery_) {
      return Indent(indent) + "[TableRef Subquery" + a + "]\n" +
             reinterpret_cast<const ASTNode *>(subquery_.get())
                 ->ToString(indent + 1);
    }
    return Indent(indent) + "[TableRef: " + table_name_ + a + "]\n";
  }
};

// E.g., JOIN orders o ON u.user_id = o.user_id_fk
struct JoinNode : public ASTNode {
  std::unique_ptr<TableRef> right_table_;
  std::unique_ptr<Expr> condition_;

  JoinNode(std::unique_ptr<TableRef> r, std::unique_ptr<Expr> c)
      : right_table_(std::move(r)), condition_(std::move(c)) {
    type_ = ASTNodeType::JOIN;
  }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[JOIN]\n";
    res += right_table_->ToString(indent + 1);
    res += Indent(indent + 1) + "- ON:\n";
    res += condition_->ToString(indent + 2);
    return res;
  }
};

// E.g., ORDER BY name DESC
struct OrderByNode : public ASTNode {
  std::unique_ptr<Expr> expr_;
  bool is_desc_; // true for DESC, false for ASC (default)

  OrderByNode(std::unique_ptr<Expr> expr, bool is_desc)
      : expr_(std::move(expr)), is_desc_(is_desc) {
    type_ = ASTNodeType::ORDER_BY;
  }

  std::string ToString(int indent = 0) const override {
    std::string dir = is_desc_ ? " DESC" : " ASC";
    return Indent(indent) + "[OrderBy:" + dir + "]\n" +
           expr_->ToString(indent + 1);
  }
};

// E.g., SUM(salary), COUNT(id)
struct AggregateExpr : public Expr {
  std::string func_name_;     // SUM, COUNT, MIN, MAX
  std::unique_ptr<Expr> arg_; // The column inside the parentheses

  AggregateExpr(std::string func, std::unique_ptr<Expr> arg)
      : func_name_(func), arg_(std::move(arg)) {
    type_ = ASTNodeType::AGGREGATE;
  }

  std::string ToString(int indent = 0) const override {
    std::string alias_suffix = alias_.empty() ? "" : " AS " + alias_;
    return Indent(indent) + "[Aggregate: " + func_name_ + "]" + alias_suffix +
           "\n" + arg_->ToString(indent + 1);
  }
};

// E.g., UPPER(name), SUBSTRING(title, 1, 5)
struct FunctionExpr : public Expr {
  std::string func_name_;
  std::vector<std::unique_ptr<Expr>> args_;

  FunctionExpr(std::string func_name, std::vector<std::unique_ptr<Expr>> args)
      : func_name_(func_name), args_(std::move(args)) {
    type_ = ASTNodeType::FUNCTION_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[Function: " + func_name_ + "]\n";
    for (size_t i = 0; i < args_.size(); ++i) {
      res += Indent(indent + 1) + "- Arg " + std::to_string(i + 1) + ":\n";
      res += args_[i]->ToString(indent + 2);
    }
    return res;
  }
};

struct CTE {
  std::string alias_;
  std::unique_ptr<SelectStatement> query_;

  CTE(std::string alias, std::unique_ptr<SelectStatement> query)
      : alias_(std::move(alias)), query_(std::move(query)) {}
};

// The Root Node: SELECT ... FROM ... WHERE ...
struct SelectStatement : public ASTNode {
  std::vector<std::unique_ptr<CTE>> ctes_;
  std::vector<std::unique_ptr<Expr>> select_list_;
  std::unique_ptr<TableRef> from_table_;
  std::vector<std::unique_ptr<JoinNode>> joins_;
  std::unique_ptr<Expr> where_clause_;
  std::vector<std::unique_ptr<Expr>> group_bys_;
  std::unique_ptr<Expr> having_clause_;
  std::vector<std::unique_ptr<OrderByNode>> order_bys_;

  bool is_distinct_{false};

  int32_t limit_count_{-1}; // -1 means no limit
  int32_t offset_count_{0};

  SelectStatement() { type_ = ASTNodeType::SELECT_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[[ SELECT STATEMENT ]]\n";
    if (!ctes_.empty()) {
      res += Indent(indent + 1) + "- WITH:\n";
      for (const auto &cte : ctes_) {
        res += Indent(indent + 2) + "[CTE: " + cte->alias_ + "]\n";
        res += cte->query_->ToString(indent + 3);
      }
    }
    res += Indent(indent + 1) + "- SELECT:\n";
    for (const auto &expr : select_list_)
      res += expr->ToString(indent + 2);

    res += Indent(indent + 1) + "- FROM:\n";
    if (from_table_)
      res += from_table_->ToString(indent + 2);

    // --- NEW: Print the JOINs ---
    for (const auto &join : joins_) {
      res += join->ToString(indent + 1);
    }

    if (where_clause_) {
      res += Indent(indent + 1) + "- WHERE:\n";
      res += where_clause_->ToString(indent + 2);
    }
    return res;
  }
};

// Helper struct for column definitions
struct ColumnDef {
  std::string name_;
  std::string type_;

  bool is_primary_key_ = false;
  bool is_not_null_ = false;
  bool is_unique_ = false;

  ColumnDef(std::string name, std::string type)
      : name_(std::move(name)), type_(std::move(type)) {}
};

struct ForeignKeyDef {
  std::string child_col_;
  std::string parent_table_;
  std::string parent_col_;

  ReferentialAction on_delete_ = ReferentialAction::RESTRICT;
  ReferentialAction on_update_ = ReferentialAction::RESTRICT;
};

struct CreateTableStatement : public ASTNode {
  std::string table_name_;
  std::vector<ColumnDef> columns_;
  std::vector<ForeignKeyDef> foreign_keys_;

  CreateTableStatement() { type_ = ASTNodeType::CREATE_TABLE_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string str = Indent(indent) + "Create Table: " + table_name_ + "\n";
    for (const auto &col : columns_) {
      str += Indent(indent + 1) + col.name_ + " [" + col.type_ + "]\n";
    }
    return str;
  }
};

struct DropTableStatement : public ASTNode {
  std::string table_name_;

  DropTableStatement(std::string name) : table_name_(std::move(name)) {
    type_ = ASTNodeType::DROP_TABLE_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    return Indent(indent) + "Drop Table: " + table_name_ + "\n";
  }
};

struct DropIndexStatement : public ASTNode {
  std::string index_name_;

  DropIndexStatement(std::string name) : index_name_(std::move(name)) {
    type_ = ASTNodeType::DROP_INDEX_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    return Indent(indent) + "Drop Index: " + index_name_ + "\n";
  }
};

struct CreateIndexStatement : public ASTNode {
  std::string index_name_;
  std::string table_name_;
  std::vector<std::string> index_columns_;
  bool is_unique_ = false;

  CreateIndexStatement() { type_ = ASTNodeType::CREATE_INDEX_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string str = Indent(indent) + "Create Index: " + index_name_ + " ON " +
                      table_name_ + "\n";
    for (const auto &col : index_columns_) {
      str += Indent(indent + 1) + col + "\n";
    }
    return str;
  }
};

// E.g., INSERT INTO users VALUES (1, 99);
struct InsertStatement : public ASTNode {
  std::string table_name_;
  std::vector<std::vector<std::unique_ptr<Expr>>>
      values_; // Supports multiple rows!

  InsertStatement() { type_ = ASTNodeType::INSERT_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[[ INSERT STATEMENT ]]\n";
    res += Indent(indent + 1) + "- INTO: " + table_name_ + "\n";
    res += Indent(indent + 1) + "- VALUES:\n";
    for (size_t i = 0; i < values_.size(); ++i) {
      res += Indent(indent + 2) + "Row " + std::to_string(i) + ":\n";
      for (const auto &expr : values_[i]) {
        res += expr->ToString(indent + 3);
      }
    }
    return res;
  }
};

// E.g., UPDATE users SET reputation = 100 WHERE user_id = 1;
struct UpdateStatement : public ASTNode {
  std::string table_name_;
  std::vector<std::pair<std::string, std::unique_ptr<Expr>>> set_clauses_;
  std::unique_ptr<Expr> where_clause_;

  UpdateStatement() { type_ = ASTNodeType::UPDATE_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[[ UPDATE STATEMENT ]]\n";
    res += Indent(indent + 1) + "- TABLE: " + table_name_ + "\n";
    res += Indent(indent + 1) + "- SET:\n";
    for (const auto &pair : set_clauses_) {
      res += Indent(indent + 2) + pair.first + " =\n";
      res += pair.second->ToString(indent + 3);
    }
    if (where_clause_) {
      res += Indent(indent + 1) + "- WHERE:\n";
      res += where_clause_->ToString(indent + 2);
    }
    return res;
  }
};

// E.g., DELETE FROM users WHERE reputation < 10;
struct DeleteStatement : public ASTNode {
  std::string table_name_;
  std::unique_ptr<Expr> where_clause_;

  DeleteStatement() { type_ = ASTNodeType::DELETE_STATEMENT; }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[[ DELETE STATEMENT ]]\n";
    res += Indent(indent + 1) + "- FROM: " + table_name_ + "\n";
    if (where_clause_) {
      res += Indent(indent + 1) + "- WHERE:\n";
      res += where_clause_->ToString(indent + 2);
    }
    return res;
  }
};

struct ExplainStatement : public ASTNode {
  std::unique_ptr<ASTNode> inner_statement_;

  ExplainStatement(std::unique_ptr<ASTNode> inner)
      : inner_statement_(std::move(inner)) {
    type_ = ASTNodeType::EXPLAIN_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    std::string res = Indent(indent) + "[[ EXPLAIN STATEMENT ]]\n";
    if (inner_statement_) {
      res += inner_statement_->ToString(indent + 1);
    }
    return res;
  }
};

// --- NEW NODE ---
struct ParameterExpr : public Expr {
  uint32_t param_idx_; // 1 for $1, 2 for $2, etc.

  ParameterExpr(uint32_t idx) : param_idx_(idx) {
    type_ = ASTNodeType::PARAMETER_EXPR;
  }

  std::string ToString(int indent = 0) const override {
    return Indent(indent) + "[Parameter: $" + std::to_string(param_idx_) +
           "]\n";
  }
};

struct SavepointStatement : public ASTNode {
  SavepointCmd cmd_;
  std::string name_;

  SavepointStatement(SavepointCmd cmd, std::string name)
      : cmd_(cmd), name_(std::move(name)) {
    type_ = ASTNodeType::SAVEPOINT_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    std::string cmd_str;
    switch (cmd_) {
    case SavepointCmd::SAVEPOINT:
      cmd_str = "SAVEPOINT";
      break;
    case SavepointCmd::RELEASE:
      cmd_str = "RELEASE SAVEPOINT";
      break;
    case SavepointCmd::ROLLBACK_TO:
      cmd_str = "ROLLBACK TO SAVEPOINT";
      break;
    }
    return Indent(indent) + "[[ " + cmd_str + ": " + name_ + " ]]\n";
  }
};

struct DeallocateStatement : public ASTNode {
  std::string name_;

  DeallocateStatement(std::string name) : name_(std::move(name)) {
    type_ = ASTNodeType::DEALLOCATE_STATEMENT;
  }

  std::string ToString(int indent = 0) const override {
    return Indent(indent) + "[[ DEALLOCATE: " + name_ + " ]]\n";
  }
};

} // namespace tetodb