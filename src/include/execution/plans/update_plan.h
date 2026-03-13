// update_plan.h

#pragma once

#include <string>
#include <unordered_map>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class UpdatePlanNode : public AbstractPlanNode {
    public:
        /**
         * @param output_schema The schema of the data we are returning
         * @param child The plan node that will feed us the tuples to update (e.g., SeqScan)
         * @param table_oid The OID of the table to update
         * @param update_exprs A map of: Column Index -> Expression AST to compute new value
         */
        UpdatePlanNode(const Schema* output_schema,
            const AbstractPlanNode* child, // <-- NEW: Added child blueprint!
            table_oid_t table_oid,
            std::unordered_map<uint32_t, const AbstractExpression*> update_exprs)
            : AbstractPlanNode(output_schema, PlanType::Update),
            child_(child), // <-- NEW: Initialize child
            table_oid_(table_oid),
            update_exprs_(std::move(update_exprs)) {
        }

        // <-- NEW: Added the getter the Execution Engine is looking for!
        inline const AbstractPlanNode* GetChildPlan() const { return child_; }

        inline table_oid_t GetTableOid() const { return table_oid_; }
        inline const std::unordered_map<uint32_t, const AbstractExpression*>& GetUpdateExpressions() const { return update_exprs_; }


        std::string ToString() const override { return "Update [Table OID: " + std::to_string(table_oid_) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }
    private:
        const AbstractPlanNode* child_; // <-- NEW: Store the child blueprint
        table_oid_t table_oid_;
        std::unordered_map<uint32_t, const AbstractExpression*> update_exprs_;
    };

} // namespace tetodb