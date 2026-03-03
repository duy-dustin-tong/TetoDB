// aggregation_plan.h

#pragma once

#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

// NO MORE EXECUTOR INCLUDE HERE! We broke the circle.

namespace tetodb {

    // MOVED FROM EXECUTOR: The blueprint now owns the Enum
    enum class AggregationType { COUNT_STAR, SUM, MIN, MAX, AVERAGE, MEDIAN };

    class AggregationPlanNode : public AbstractPlanNode {
    public:
        AggregationPlanNode(const Schema* output_schema,
            const AbstractPlanNode* child,
            std::vector<const AbstractExpression*> group_bys,
            std::vector<const AbstractExpression*> aggregates,
            std::vector<AggregationType> agg_types)
            : AbstractPlanNode(output_schema, PlanType::Aggregation),
            child_(child),
            group_bys_(std::move(group_bys)),
            aggregates_(std::move(aggregates)),
            agg_types_(std::move(agg_types)) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline const std::vector<const AbstractExpression*>& GetGroupBys() const { return group_bys_; }
        inline const std::vector<const AbstractExpression*>& GetAggregates() const { return aggregates_; }
        inline const std::vector<AggregationType>& GetAggregateTypes() const { return agg_types_; }


        std::string ToString() const override { return "Aggregation"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }
    private:
        const AbstractPlanNode* child_;
        std::vector<const AbstractExpression*> group_bys_;
        std::vector<const AbstractExpression*> aggregates_;
        std::vector<AggregationType> agg_types_;
    };

} // namespace tetodb