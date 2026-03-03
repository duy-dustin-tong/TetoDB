// planner.h

#pragma once

#include <memory>
#include <vector>
#include <stdexcept>
#include <unordered_map>
#include <string>

#include "parser/ast.h"
#include "catalog/catalog.h"
#include "execution/execution_context.h"
#include "execution/expressions/abstract_expression.h"

// --- ONLY INCLUDE PLAN NODES, NO EXECUTORS ---
#include "execution/plans/abstract_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/update_plan.h" 
#include "execution/plans/delete_plan.h" 
#include "execution/plans/filter_plan.h" 
#include "execution/expressions/comparison_expression.h"

namespace tetodb {

    class Planner {
    public:
        Planner(Catalog* catalog, ExecutionContext* exec_ctx)
            : catalog_(catalog), exec_ctx_(exec_ctx) {
        }

        const AbstractPlanNode* PlanQuery(const ASTNode* ast);
    private:
        const AbstractPlanNode* PlanSelect(const SelectStatement* stmt);
        const AbstractPlanNode* PlanInsert(const InsertStatement* stmt);
        const AbstractPlanNode* PlanUpdate(const UpdateStatement* stmt);
        const AbstractPlanNode* PlanDelete(const DeleteStatement* stmt);

        // --- NEW: Added alias_map parameter for alias resolution ---
        std::unique_ptr<AbstractExpression> PlanExpression(const Expr* ast_expr, const Schema* schema, const std::unordered_map<std::string, std::string>& alias_map = {});

        Catalog* catalog_;
        ExecutionContext* exec_ctx_;

        std::string active_left_alias_;
        std::string active_right_alias_;
        const Schema* active_left_schema_ = nullptr;
        const Schema* active_right_schema_ = nullptr;

        // --- Memory Arenas ---
        std::vector<std::unique_ptr<AbstractExpression>> expressions_;
        std::vector<std::unique_ptr<Schema>> schemas_;
        std::vector<std::unique_ptr<AbstractPlanNode>> plan_nodes_;
    };

} // namespace tetodb