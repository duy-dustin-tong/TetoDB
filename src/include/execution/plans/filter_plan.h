// filter_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class FilterPlanNode : public AbstractPlanNode {
    public:
        FilterPlanNode(const Schema* output_schema,
            const AbstractPlanNode* child,
            const AbstractExpression* predicate)
            : AbstractPlanNode(output_schema, PlanType::Filter),
            child_(child),
            predicate_(predicate) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline const AbstractExpression* GetPredicate() const { return predicate_; }

        std::string ToString() const override { return "Filter"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }

    private:
        const AbstractPlanNode* child_;
        const AbstractExpression* predicate_;
    };

} // namespace tetodb