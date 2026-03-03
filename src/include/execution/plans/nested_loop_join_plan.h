// nested_loop_join_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class NestedLoopJoinPlanNode : public AbstractPlanNode {
    public:
        NestedLoopJoinPlanNode(const Schema* output_schema,
            const AbstractPlanNode* left_child,
            const AbstractPlanNode* right_child,
            const AbstractExpression* predicate)
            : AbstractPlanNode(output_schema, PlanType::NestedLoopJoin),
            left_child_(left_child),
            right_child_(right_child),
            predicate_(predicate) {
        }

        inline const AbstractPlanNode* GetLeftPlan() const { return left_child_; }
        inline const AbstractPlanNode* GetRightPlan() const { return right_child_; }
        inline const AbstractExpression* Predicate() const { return predicate_; }

        std::string ToString() const override { return "NestedLoopJoin"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { left_child_, right_child_ }; }

    private:
        const AbstractPlanNode* left_child_;
        const AbstractPlanNode* right_child_;
        const AbstractExpression* predicate_;
    };

} // namespace tetodb