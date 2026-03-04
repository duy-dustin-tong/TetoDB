// planner.cpp

// planner.cpp

#include "planner/planner.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/in_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/parameter_value_expression.h"
#include "execution/expressions/string_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include <unordered_map>

namespace tetodb {

// --- UPDATED: Take raw pointer ---
const AbstractPlanNode *Planner::PlanQuery(const ASTNode *ast) {
  if (!ast)
    throw std::runtime_error("Planner Error: AST is null");

  if (ast->type_ == ASTNodeType::SELECT_STATEMENT) {
    return PlanSelect(static_cast<const SelectStatement *>(ast));
  } else if (ast->type_ == ASTNodeType::INSERT_STATEMENT) {
    return PlanInsert(static_cast<const InsertStatement *>(ast));
  } else if (ast->type_ == ASTNodeType::UPDATE_STATEMENT) {
    return PlanUpdate(static_cast<const UpdateStatement *>(ast));
  } else if (ast->type_ == ASTNodeType::DELETE_STATEMENT) {
    return PlanDelete(static_cast<const DeleteStatement *>(ast));
  }
  throw std::runtime_error("Planner Error: Unsupported AST Node Type");
}

const AbstractPlanNode *Planner::PlanSelect(const SelectStatement *stmt) {
  if (!stmt->from_table_) {
    throw std::runtime_error("Planner Error: SELECT must have a FROM clause");
  }

  std::string left_table_name = stmt->from_table_->table_name_;
  TableMetadata *left_meta = catalog_->GetTable(left_table_name);
  if (!left_meta)
    throw std::runtime_error("Planner Error: Table '" + left_table_name +
                             "' not found.");

  // --- STORE LEFT ALIAS CONTEXT ---
  active_left_alias_ = stmt->from_table_->alias_.empty()
                           ? left_table_name
                           : stmt->from_table_->alias_;
  active_left_schema_ = &left_meta->schema_;
  active_right_alias_ = "";
  active_right_schema_ = nullptr;

  auto left_scan =
      std::make_unique<SeqScanPlanNode>(&left_meta->schema_, left_meta->oid_);
  const AbstractPlanNode *current_plan = left_scan.get();
  const Schema *current_schema = &left_meta->schema_;
  plan_nodes_.push_back(std::move(left_scan));

  // --- JOINS ---
  if (!stmt->joins_.empty()) {
    const auto *join_ast = stmt->joins_[0].get();

    std::string right_table_name = join_ast->right_table_->table_name_;
    TableMetadata *right_meta = catalog_->GetTable(right_table_name);
    if (!right_meta)
      throw std::runtime_error("Planner Error: Table '" + right_table_name +
                               "' not found.");

    // --- STORE RIGHT ALIAS CONTEXT ---
    active_right_alias_ = join_ast->right_table_->alias_.empty()
                              ? right_table_name
                              : join_ast->right_table_->alias_;
    active_right_schema_ = &right_meta->schema_;

    auto right_scan = std::make_unique<SeqScanPlanNode>(&right_meta->schema_,
                                                        right_meta->oid_);
    const AbstractPlanNode *right_scan_ptr = right_scan.get();
    plan_nodes_.push_back(std::move(right_scan));

    if (join_ast->condition_->type_ != ASTNodeType::BINARY_EXPR) {
      throw std::runtime_error(
          "Planner Error: JOIN condition must be a binary expression");
    }
    const auto *bin_expr =
        static_cast<const BinaryExpr *>(join_ast->condition_.get());

    const auto *left_col_ast =
        static_cast<const ColumnRefExpr *>(bin_expr->left_.get());
    const auto *right_col_ast =
        static_cast<const ColumnRefExpr *>(bin_expr->right_.get());

    uint32_t left_col_idx =
        left_meta->schema_.GetColIdx(left_col_ast->col_name_);
    uint32_t right_col_idx =
        right_meta->schema_.GetColIdx(right_col_ast->col_name_);

    auto left_key_expr =
        std::make_unique<ColumnValueExpression>(0, left_col_idx);
    auto right_key_expr =
        std::make_unique<ColumnValueExpression>(1, right_col_idx);

    static const std::unordered_map<std::string, CompType> comp_map = {
        {"=", CompType::EQUAL},
        {"<", CompType::LESS_THAN},
        {">", CompType::GREATER_THAN},
        {"<=", CompType::LESS_THAN_OR_EQUAL},
        {">=", CompType::GREATER_THAN_OR_EQUAL},
        {"!=", CompType::NOT_EQUAL},
        {"IS_NULL", CompType::IS_NULL},
        {"IS_NOT_NULL", CompType::IS_NOT_NULL}};
    auto comp_it = comp_map.find(bin_expr->op_);
    if (comp_it == comp_map.end())
      throw std::runtime_error("Planner Error: Unsupported JOIN operator '" +
                               bin_expr->op_ + "'");

    auto comp_expr = std::make_unique<ComparisonExpression>(
        comp_it->second, std::move(left_key_expr), std::move(right_key_expr));
    const AbstractExpression *predicate_ptr = comp_expr.get();
    expressions_.push_back(std::move(comp_expr));

    std::vector<Column> joined_cols = left_meta->schema_.GetColumns();
    for (const auto &col : right_meta->schema_.GetColumns()) {
      joined_cols.push_back(col);
    }
    auto joined_schema = std::make_unique<Schema>(joined_cols);
    current_schema = joined_schema.get();
    schemas_.push_back(std::move(joined_schema));

    auto nlj = std::make_unique<NestedLoopJoinPlanNode>(
        current_schema, current_plan, right_scan_ptr, predicate_ptr);
    current_plan = nlj.get();
    plan_nodes_.push_back(std::move(nlj));
  }

  // ==========================================
  // ALIAS RESOLUTION MAP
  // ==========================================
  std::unordered_map<std::string, std::string> alias_map;
  for (const auto &expr_ast : stmt->select_list_) {
    if (expr_ast->type_ == ASTNodeType::COLUMN_REF) {
      const auto *col_ref = static_cast<const ColumnRefExpr *>(expr_ast.get());
      if (!col_ref->alias_.empty())
        alias_map[col_ref->alias_] = col_ref->col_name_;
    } else if (expr_ast->type_ == ASTNodeType::AGGREGATE) {
      const auto *agg_expr = static_cast<const AggregateExpr *>(expr_ast.get());
      if (!agg_expr->alias_.empty())
        alias_map[agg_expr->alias_] = agg_expr->func_name_;
    }
  }

  // --- THE WHERE CLAUSE FILTER ---
  if (stmt->where_clause_) {
    auto filter_expr =
        PlanExpression(stmt->where_clause_.get(), current_schema, alias_map);
    const AbstractExpression *predicate_ptr = filter_expr.get();
    expressions_.push_back(std::move(filter_expr));

    auto filter_plan = std::make_unique<FilterPlanNode>(
        current_schema, current_plan, predicate_ptr);
    current_plan = filter_plan.get();
    plan_nodes_.push_back(std::move(filter_plan));
  }

  // --- THE AGGREGATION FUNNEL ---
  bool has_aggregation = !stmt->group_bys_.empty();
  for (const auto &expr_ast : stmt->select_list_) {
    if (expr_ast->type_ == ASTNodeType::AGGREGATE)
      has_aggregation = true;
  }

  if (has_aggregation) {
    std::vector<const AbstractExpression *> group_bys;
    std::vector<const AbstractExpression *> aggregates;
    std::vector<AggregationType> agg_types;
    std::vector<Column> agg_out_cols;

    for (const auto &gb_ast : stmt->group_bys_) {
      auto gb_expr = PlanExpression(gb_ast.get(), current_schema, alias_map);
      if (gb_ast->type_ == ASTNodeType::COLUMN_REF) {
        std::string col_name =
            static_cast<const ColumnRefExpr *>(gb_ast.get())->col_name_;
        uint32_t col_idx = current_schema->GetColIdx(col_name);
        agg_out_cols.push_back(current_schema->GetColumn(col_idx));
      }
      group_bys.push_back(gb_expr.get());
      expressions_.push_back(std::move(gb_expr));
    }

    static const std::unordered_map<std::string, AggregationType> agg_map = {
        {"COUNT", AggregationType::COUNT_STAR},
        {"SUM", AggregationType::SUM},
        {"MIN", AggregationType::MIN},
        {"MAX", AggregationType::MAX},
        {"AVG", AggregationType::AVERAGE},
        {"AVERAGE", AggregationType::AVERAGE},
        {"MED", AggregationType::MEDIAN},
        {"MEDIAN", AggregationType::MEDIAN}};

    for (const auto &expr_ast : stmt->select_list_) {
      if (expr_ast->type_ == ASTNodeType::AGGREGATE) {
        const auto *agg_ast =
            static_cast<const AggregateExpr *>(expr_ast.get());
        auto arg_expr =
            PlanExpression(agg_ast->arg_.get(), current_schema, alias_map);

        auto it = agg_map.find(agg_ast->func_name_);
        if (it == agg_map.end())
          throw std::runtime_error(
              "Planner Error: Unknown aggregate function: " +
              agg_ast->func_name_);

        AggregationType atype = it->second;
        TypeId return_type = (atype == AggregationType::AVERAGE ||
                              atype == AggregationType::MEDIAN)
                                 ? TypeId::DECIMAL
                                 : TypeId::INTEGER;

        aggregates.push_back(arg_expr.get());
        expressions_.push_back(std::move(arg_expr));
        agg_types.push_back(atype);
        agg_out_cols.push_back(Column(agg_ast->func_name_, return_type));
      }
    }

    auto agg_schema = std::make_unique<Schema>(agg_out_cols);
    current_schema = agg_schema.get();
    schemas_.push_back(std::move(agg_schema));

    auto agg_plan = std::make_unique<AggregationPlanNode>(
        current_schema, current_plan, std::move(group_bys),
        std::move(aggregates), std::move(agg_types));
    current_plan = agg_plan.get();
    plan_nodes_.push_back(std::move(agg_plan));
  }

  // --- THE HAVING FILTER ---
  if (stmt->having_clause_) {
    // We cannot have HAVING without Aggregation.
    if (!has_aggregation) {
      throw std::runtime_error(
          "Planner Error: HAVING requires GROUP BY or Aggregation functions");
    }

    auto filter_expr =
        PlanExpression(stmt->having_clause_.get(), current_schema, alias_map);
    const AbstractExpression *predicate_ptr = filter_expr.get();
    expressions_.push_back(std::move(filter_expr));

    auto filter_plan = std::make_unique<FilterPlanNode>(
        current_schema, current_plan, predicate_ptr);
    current_plan = filter_plan.get();
    plan_nodes_.push_back(std::move(filter_plan));
  }

  // --- THE ORDER BY SORTING ---
  if (!stmt->order_bys_.empty()) {
    std::vector<std::pair<OrderByType, const AbstractExpression *>> order_bys;
    for (const auto &ob : stmt->order_bys_) {
      auto ob_expr = PlanExpression(ob->expr_.get(), current_schema, alias_map);
      OrderByType type = ob->is_desc_ ? OrderByType::DESC : OrderByType::ASC;
      order_bys.push_back({type, ob_expr.get()});
      expressions_.push_back(std::move(ob_expr));
    }
    auto sort_plan = std::make_unique<SortPlanNode>(
        current_schema, current_plan, std::move(order_bys));
    current_plan = sort_plan.get();
    plan_nodes_.push_back(std::move(sort_plan));
  }

  // --- PROJECTION WITH ALIAS-AWARE OFFSETS ---
  std::vector<const AbstractExpression *> proj_exprs;
  std::vector<Column> out_columns;

  for (const auto &expr_ast : stmt->select_list_) {
    // 1. Check for `SELECT *` expansion
    if (expr_ast->type_ == ASTNodeType::COLUMN_REF) {
      const auto *col_ref = static_cast<const ColumnRefExpr *>(expr_ast.get());
      if (col_ref->col_name_ == "*") {
        for (uint32_t i = 0; i < current_schema->GetColumnCount(); i++) {
          const Column &col = current_schema->GetColumn(i);
          out_columns.push_back(col);

          auto col_expr = std::make_unique<ColumnValueExpression>(0, i);
          proj_exprs.push_back(col_expr.get());
          expressions_.push_back(std::move(col_expr));
        }
        continue;
      }
    }

    // 2. Generic Expression Evaluation
    std::string alias = "";
    if (expr_ast->type_ == ASTNodeType::COLUMN_REF) {
      alias = static_cast<const ColumnRefExpr *>(expr_ast.get())->alias_;
    } else if (expr_ast->type_ == ASTNodeType::AGGREGATE) {
      alias = static_cast<const AggregateExpr *>(expr_ast.get())->alias_;
    }

    auto expr = PlanExpression(expr_ast.get(), current_schema, alias_map);
    TypeId return_type = expr->GetReturnType();

    // Attempt to formulate a default column name if no alias was provided
    std::string col_name = alias;
    if (col_name.empty()) {
      if (expr_ast->type_ == ASTNodeType::COLUMN_REF) {
        col_name =
            static_cast<const ColumnRefExpr *>(expr_ast.get())->col_name_;
      } else if (expr_ast->type_ == ASTNodeType::AGGREGATE) {
        col_name =
            static_cast<const AggregateExpr *>(expr_ast.get())->func_name_;
      } else if (expr_ast->type_ == ASTNodeType::FUNCTION_EXPR) {
        col_name =
            static_cast<const FunctionExpr *>(expr_ast.get())->func_name_;
      } else {
        col_name = "expr";
      }
    }

    out_columns.push_back(Column(col_name, return_type));
    proj_exprs.push_back(expr.get());
    expressions_.push_back(std::move(expr));
  }

  auto out_schema = std::make_unique<Schema>(out_columns);
  const Schema *out_schema_ptr = out_schema.get();
  schemas_.push_back(std::move(out_schema));

  auto proj_plan = std::make_unique<ProjectionPlanNode>(
      out_schema_ptr, current_plan, proj_exprs);
  const AbstractPlanNode *current_root = proj_plan.get();
  plan_nodes_.push_back(std::move(proj_plan));

  // --- THE LIMIT WRAPPER ---
  if (stmt->limit_count_ != -1 || stmt->offset_count_ > 0) {
    auto limit_plan = std::make_unique<LimitPlanNode>(
        out_schema_ptr, current_root, stmt->limit_count_, stmt->offset_count_);
    current_root = limit_plan.get();
    plan_nodes_.push_back(std::move(limit_plan));
  }

  // --- THE DISTINCT WRAPPER ---
  if (stmt->is_distinct_) {
    auto distinct_plan =
        std::make_unique<DistinctPlanNode>(out_schema_ptr, current_root);
    current_root = distinct_plan.get();
    plan_nodes_.push_back(std::move(distinct_plan));
  }

  return current_root;
}

const AbstractPlanNode *Planner::PlanInsert(const InsertStatement *stmt) {
  std::string table_name = stmt->table_name_;
  TableMetadata *table_meta = catalog_->GetTable(table_name);
  if (!table_meta)
    throw std::runtime_error("Planner Error: Table '" + table_name +
                             "' not found.");

  const Schema &schema = table_meta->schema_;
  std::vector<std::vector<const AbstractExpression *>> insert_exprs;

  for (const auto &row_exprs : stmt->values_) {
    if (row_exprs.size() != schema.GetColumnCount())
      throw std::runtime_error(
          "Planner Error: INSERT values count does not match schema.");

    std::vector<const AbstractExpression *> row;
    for (size_t col_idx = 0; col_idx < row_exprs.size(); ++col_idx) {
      auto expr = PlanExpression(row_exprs[col_idx].get(), &schema);
      row.push_back(expr.get());
      expressions_.push_back(std::move(expr));
    }
    insert_exprs.push_back(row);
  }

  auto insert_plan = std::make_unique<InsertPlanNode>(table_meta->oid_,
                                                      std::move(insert_exprs));
  const AbstractPlanNode *plan_ptr = insert_plan.get();
  plan_nodes_.push_back(std::move(insert_plan));
  return plan_ptr;
}
const AbstractPlanNode *Planner::PlanUpdate(const UpdateStatement *stmt) {
  std::string table_name = stmt->table_name_;
  TableMetadata *table_meta = catalog_->GetTable(table_name);
  if (!table_meta)
    throw std::runtime_error("Planner Error: Table '" + table_name +
                             "' not found.");

  const Schema &schema = table_meta->schema_;
  auto child_scan =
      std::make_unique<SeqScanPlanNode>(&schema, table_meta->oid_);
  const AbstractPlanNode *current_plan = child_scan.get();
  plan_nodes_.push_back(std::move(child_scan));

  if (stmt->where_clause_) {
    auto filter_expr = PlanExpression(stmt->where_clause_.get(), &schema);
    const AbstractExpression *predicate_ptr = filter_expr.get();
    expressions_.push_back(std::move(filter_expr));

    auto filter_plan =
        std::make_unique<FilterPlanNode>(&schema, current_plan, predicate_ptr);
    current_plan = filter_plan.get();
    plan_nodes_.push_back(std::move(filter_plan));
  }

  std::unordered_map<uint32_t, const AbstractExpression *> update_exprs;
  for (const auto &set_clause : stmt->set_clauses_) {
    std::string col_name = set_clause.first;
    uint32_t col_idx = schema.GetColIdx(col_name);

    if (col_idx == static_cast<uint32_t>(-1))
      throw std::runtime_error("Planner Error: Column '" + col_name +
                               "' not found.");

    const auto *ast_const =
        dynamic_cast<const ConstantExpr *>(set_clause.second.get());
    if (!ast_const)
      throw std::runtime_error(
          "Planner Error: UPDATE SET only supports raw constants right now.");

    TypeId expected_type = schema.GetColumn(col_idx).GetTypeId();
    std::string raw_str = ast_const->value_;
    Value val;

    if (raw_str == "NULL") {
      val = Value::GetNullValue(expected_type);
    } else if (expected_type == TypeId::INTEGER)
      val = Value(TypeId::INTEGER, std::stoi(raw_str));
    else if (expected_type == TypeId::BIGINT)
      val = Value(TypeId::BIGINT, static_cast<int64_t>(std::stoll(raw_str)));
    else if (expected_type == TypeId::DECIMAL)
      val = Value(TypeId::DECIMAL, std::stod(raw_str));
    else if (expected_type == TypeId::BOOLEAN) {
      bool b = (raw_str == "TRUE" || raw_str == "true" || raw_str == "1");
      val = Value(TypeId::BOOLEAN, b);
    } else if (expected_type == TypeId::TIMESTAMP)
      val = Value(TypeId::BIGINT, static_cast<int64_t>(std::stoll(raw_str)));
    else if (expected_type == TypeId::VARCHAR) {
      if (raw_str.length() >= 2 && raw_str.front() == '\'' &&
          raw_str.back() == '\'') {
        raw_str = raw_str.substr(1, raw_str.length() - 2);
      }
      val = Value(TypeId::VARCHAR, raw_str);
    } else {
      val = Value(TypeId::VARCHAR, raw_str);
    }

    auto const_val_expr = std::make_unique<ConstantValueExpression>(val);
    update_exprs[col_idx] = const_val_expr.get();
    expressions_.push_back(std::move(const_val_expr));
  }

  auto update_plan = std::make_unique<UpdatePlanNode>(
      &schema, current_plan, table_meta->oid_, std::move(update_exprs));
  const AbstractPlanNode *plan_ptr = update_plan.get();
  plan_nodes_.push_back(std::move(update_plan));

  return plan_ptr;
}

const AbstractPlanNode *Planner::PlanDelete(const DeleteStatement *stmt) {
  std::string table_name = stmt->table_name_;
  TableMetadata *table_meta = catalog_->GetTable(table_name);
  if (!table_meta)
    throw std::runtime_error("Planner Error: Table '" + table_name +
                             "' not found.");

  auto child_scan =
      std::make_unique<SeqScanPlanNode>(&table_meta->schema_, table_meta->oid_);
  const AbstractPlanNode *current_plan = child_scan.get();
  plan_nodes_.push_back(std::move(child_scan));

  if (stmt->where_clause_) {
    auto filter_expr =
        PlanExpression(stmt->where_clause_.get(), &table_meta->schema_);
    const AbstractExpression *predicate_ptr = filter_expr.get();
    expressions_.push_back(std::move(filter_expr));

    auto filter_plan = std::make_unique<FilterPlanNode>(
        &table_meta->schema_, current_plan, predicate_ptr);
    current_plan = filter_plan.get();
    plan_nodes_.push_back(std::move(filter_plan));
  }

  auto delete_plan =
      std::make_unique<DeletePlanNode>(current_plan, table_meta->oid_);
  const AbstractPlanNode *plan_ptr = delete_plan.get();
  plan_nodes_.push_back(std::move(delete_plan));

  return plan_ptr;
}

// --- ALIAS RESOLUTION INJECTED HERE ---
std::unique_ptr<AbstractExpression> Planner::PlanExpression(
    const Expr *ast_expr, const Schema *schema,
    const std::unordered_map<std::string, std::string> &alias_map) {

  if (const auto *col_ref = dynamic_cast<const ColumnRefExpr *>(ast_expr)) {
    std::string target_col = col_ref->col_name_;

    if (target_col == "*") {
      return std::make_unique<ConstantValueExpression>(
          Value(TypeId::INTEGER, 1));
    }

    if (alias_map.find(target_col) != alias_map.end()) {
      target_col = alias_map.at(target_col);
    }

    uint32_t col_idx = static_cast<uint32_t>(-1);

    if (!col_ref->table_alias_.empty()) {
      if (col_ref->table_alias_ == active_right_alias_ &&
          active_right_schema_) {
        uint32_t local_idx = active_right_schema_->GetColIdx(target_col);
        if (local_idx != static_cast<uint32_t>(-1)) {
          col_idx = active_left_schema_->GetColumnCount() + local_idx;
        }
      } else if (col_ref->table_alias_ == active_left_alias_ &&
                 active_left_schema_) {
        col_idx = active_left_schema_->GetColIdx(target_col);
      }
    }

    if (col_idx == static_cast<uint32_t>(-1)) {
      col_idx = schema->GetColIdx(target_col);
    }

    if (col_idx == static_cast<uint32_t>(-1)) {
      throw std::runtime_error("Planner Error: Column/Alias '" +
                               col_ref->col_name_ + "' not found.");
    }
    TypeId return_type = schema->GetColumn(col_idx).GetTypeId();
    return std::make_unique<ColumnValueExpression>(0, col_idx, return_type);
  }

  if (const auto *const_expr = dynamic_cast<const ConstantExpr *>(ast_expr)) {
    Value val;
    std::string raw_str = const_expr->value_;
    if (raw_str == "NULL") {
      val = Value::GetNullValue(TypeId::VARCHAR);
    } else if (raw_str.length() >= 2 && raw_str.front() == '\'' &&
               raw_str.back() == '\'') {
      val = Value(TypeId::VARCHAR, raw_str.substr(1, raw_str.length() - 2));
    } else if (raw_str == "TRUE" || raw_str == "true" || raw_str == "FALSE" ||
               raw_str == "false") {
      bool b = (raw_str == "TRUE" || raw_str == "true");
      val = Value(TypeId::BOOLEAN, b);
    } else {
      try {
        if (raw_str.find('.') != std::string::npos) {
          val = Value(TypeId::DECIMAL, std::stod(raw_str));
        } else {
          val = Value(TypeId::INTEGER, std::stoi(raw_str));
        }
      } catch (...) {
        val = Value(TypeId::VARCHAR, raw_str);
      }
    }
    return std::make_unique<ConstantValueExpression>(val);
  }

  if (const auto *param_expr = dynamic_cast<const ParameterExpr *>(ast_expr)) {
    return std::make_unique<ParameterValueExpression>(param_expr->param_idx_);
  }

  if (const auto *agg_expr = dynamic_cast<const AggregateExpr *>(ast_expr)) {
    std::string target_name = agg_expr->func_name_;
    if (alias_map.find(agg_expr->alias_) != alias_map.end()) {
      target_name = alias_map.at(agg_expr->alias_);
    }

    uint32_t col_idx = schema->GetColIdx(target_name);
    if (col_idx == static_cast<uint32_t>(-1))
      throw std::runtime_error("Planner Error: Aggregate '" + target_name +
                               "' not found in schema.");
    TypeId return_type = schema->GetColumn(col_idx).GetTypeId();
    return std::make_unique<ColumnValueExpression>(0, col_idx, return_type);
  }

  if (const auto *bin_expr = dynamic_cast<const BinaryExpr *>(ast_expr)) {
    auto left = PlanExpression(bin_expr->left_.get(), schema, alias_map);
    auto right = PlanExpression(bin_expr->right_.get(), schema, alias_map);

    static const std::unordered_map<std::string, CompType> comp_map = {
        {"=", CompType::EQUAL},
        {"<", CompType::LESS_THAN},
        {">", CompType::GREATER_THAN},
        {"<=", CompType::LESS_THAN_OR_EQUAL},
        {">=", CompType::GREATER_THAN_OR_EQUAL},
        {"!=", CompType::NOT_EQUAL},
        {"IS_NULL", CompType::IS_NULL},
        {"IS_NOT_NULL", CompType::IS_NOT_NULL},
        {"LIKE", CompType::LIKE},
        {"ILIKE", CompType::ILIKE}};
    auto it = comp_map.find(bin_expr->op_);
    if (it == comp_map.end())
      throw std::runtime_error("Planner Error: Unsupported operator '" +
                               bin_expr->op_ + "'");

    return std::make_unique<ComparisonExpression>(it->second, std::move(left),
                                                  std::move(right));
  }

  if (const auto *func_expr = dynamic_cast<const FunctionExpr *>(ast_expr)) {
    static const std::unordered_map<std::string, StringFuncType> func_map = {
        {"UPPER", StringFuncType::UPPER},
        {"LOWER", StringFuncType::LOWER},
        {"LENGTH", StringFuncType::LENGTH},
        {"CONCAT", StringFuncType::CONCAT},
        {"SUBSTRING", StringFuncType::SUBSTRING}};

    auto it = func_map.find(func_expr->func_name_);
    if (it == func_map.end()) {
      throw std::runtime_error("Planner Error: Unsupported function '" +
                               func_expr->func_name_ + "'");
    }

    std::vector<std::unique_ptr<AbstractExpression>> args;
    for (const auto &arg : func_expr->args_) {
      args.push_back(PlanExpression(arg.get(), schema, alias_map));
    }

    return std::make_unique<StringExpression>(it->second, std::move(args));
  }

  if (const auto *log_expr = dynamic_cast<const LogicalExpr *>(ast_expr)) {
    auto left = PlanExpression(log_expr->left_.get(), schema, alias_map);
    auto right = PlanExpression(log_expr->right_.get(), schema, alias_map);
    LogicType type = log_expr->op_ == "AND" ? LogicType::AND : LogicType::OR;
    return std::make_unique<LogicExpression>(type, std::move(left),
                                             std::move(right));
  }

  if (const auto *not_expr = dynamic_cast<const NotExpr *>(ast_expr)) {
    auto child = PlanExpression(not_expr->child_.get(), schema, alias_map);
    return std::make_unique<LogicExpression>(LogicType::NOT, std::move(child));
  }

  if (const auto *in_expr = dynamic_cast<const InExpr *>(ast_expr)) {
    auto left = PlanExpression(in_expr->left_.get(), schema, alias_map);
    std::vector<std::unique_ptr<AbstractExpression>> list;
    for (const auto &item : in_expr->in_list_) {
      list.push_back(PlanExpression(item.get(), schema, alias_map));
    }
    return std::make_unique<InExpression>(std::move(left), std::move(list),
                                          in_expr->is_not_);
  }

  if (const auto *btw_expr = dynamic_cast<const BetweenExpr *>(ast_expr)) {
    auto target1 = PlanExpression(btw_expr->expr_.get(), schema, alias_map);
    auto target2 = PlanExpression(btw_expr->expr_.get(), schema, alias_map);

    auto lower = PlanExpression(btw_expr->lower_.get(), schema, alias_map);
    auto upper = PlanExpression(btw_expr->upper_.get(), schema, alias_map);

    std::unique_ptr<AbstractExpression> comp1 =
        std::make_unique<ComparisonExpression>(CompType::GREATER_THAN_OR_EQUAL,
                                               std::move(target1),
                                               std::move(lower));
    std::unique_ptr<AbstractExpression> comp2 =
        std::make_unique<ComparisonExpression>(
            CompType::LESS_THAN_OR_EQUAL, std::move(target2), std::move(upper));

    std::unique_ptr<AbstractExpression> logical_and =
        std::make_unique<LogicExpression>(LogicType::AND, std::move(comp1),
                                          std::move(comp2));

    if (btw_expr->is_not_) {
      return std::make_unique<LogicExpression>(LogicType::NOT,
                                               std::move(logical_and));
    }
    return logical_and;
  }

  throw std::runtime_error("Planner Error: Unsupported expression type");
}

} // namespace tetodb