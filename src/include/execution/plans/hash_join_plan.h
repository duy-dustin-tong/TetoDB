// hash_join_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class HashJoinPlanNode : public AbstractPlanNode {
    public:
        HashJoinPlanNode(const Schema* output_schema,
            const AbstractPlanNode* left_child,
            const AbstractPlanNode* right_child,
            const AbstractExpression* left_key_expr,
            const AbstractExpression* right_key_expr)
            : AbstractPlanNode(output_schema, PlanType::HashJoin),
            left_child_(left_child),
            right_child_(right_child),
            left_key_expr_(left_key_expr),
            right_key_expr_(right_key_expr) {
        }

        inline const AbstractPlanNode* GetLeftPlan() const { return left_child_; }
        inline const AbstractPlanNode* GetRightPlan() const { return right_child_; }
        inline const AbstractExpression* LeftJoinKeyExpression() const { return left_key_expr_; }
        inline const AbstractExpression* RightJoinKeyExpression() const { return right_key_expr_; }

        std::string ToString() const override { return "HashJoin"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { left_child_, right_child_ }; }

    private:
        const AbstractPlanNode* left_child_;
        const AbstractPlanNode* right_child_;
        const AbstractExpression* left_key_expr_;
        const AbstractExpression* right_key_expr_;
    };

} // namespace tetodb

