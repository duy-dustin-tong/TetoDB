// topn_plan.h

#pragma once

#include <vector>
#include <utility>
#include "execution/plans/abstract_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class TopNPlanNode : public AbstractPlanNode {
    public:
        TopNPlanNode(const Schema* output_schema,
            const AbstractPlanNode* child,
            std::vector<std::pair<OrderByType, const AbstractExpression*>> order_bys,
            int32_t limit,
            int32_t offset)
            : AbstractPlanNode(output_schema, PlanType::TopN),
            child_(child),
            order_bys_(std::move(order_bys)),
            limit_(limit),
            offset_(offset) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline const std::vector<std::pair<OrderByType, const AbstractExpression*>>& GetOrderBys() const { return order_bys_; }
        inline int32_t GetLimit() const { return limit_; }
        inline int32_t GetOffset() const { return offset_; }

        std::string ToString() const override { return "TopN [Limit: " + std::to_string(limit_) + ", Offset: " + std::to_string(offset_) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }

    private:
        const AbstractPlanNode* child_;
        std::vector<std::pair<OrderByType, const AbstractExpression*>> order_bys_;
        int32_t limit_;
        int32_t offset_;
    };

} // namespace tetodb