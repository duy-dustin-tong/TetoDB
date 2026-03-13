// sort_plan.h

#pragma once

#include <vector>
#include <utility>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    enum class OrderByType { DEFAULT, ASC, DESC };

    class SortPlanNode : public AbstractPlanNode {
    public:
        SortPlanNode(const Schema* output_schema,
            const AbstractPlanNode* child,
            std::vector<std::pair<OrderByType, const AbstractExpression*>> order_bys)
            : AbstractPlanNode(output_schema, PlanType::Sort),
            child_(child),
            order_bys_(std::move(order_bys)) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline const std::vector<std::pair<OrderByType, const AbstractExpression*>>& GetOrderBys() const { return order_bys_; }


        std::string ToString() const override { return "Sort"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }
    private:
        const AbstractPlanNode* child_;
        // A list of expressions to sort by, in order of priority (e.g., ORDER BY age DESC, name ASC)
        std::vector<std::pair<OrderByType, const AbstractExpression*>> order_bys_;
    };

} // namespace tetodb