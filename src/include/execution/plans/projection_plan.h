// projection_plan.h

#pragma once

#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class ProjectionPlanNode : public AbstractPlanNode {
    public:
        ProjectionPlanNode(const Schema* output_schema,
            const AbstractPlanNode* child,
            std::vector<const AbstractExpression*> expressions)
            : AbstractPlanNode(output_schema, PlanType::Projection),
            child_(child),
            expressions_(std::move(expressions)) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline const std::vector<const AbstractExpression*>& GetExpressions() const { return expressions_; }


        std::string ToString() const override { return "Projection"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }
    private:
        const AbstractPlanNode* child_;
        std::vector<const AbstractExpression*> expressions_;
    };

} // namespace tetodb