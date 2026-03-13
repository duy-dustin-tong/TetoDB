// limit_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"

namespace tetodb {

    class LimitPlanNode : public AbstractPlanNode {
    public:
        LimitPlanNode(const Schema* output_schema, const AbstractPlanNode* child, int32_t limit, int32_t offset)
            : AbstractPlanNode(output_schema, PlanType::Limit), child_(child), limit_(limit), offset_(offset) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline int32_t GetLimit() const { return limit_; }
        inline int32_t GetOffset() const { return offset_; }

        std::string ToString() const override { return "Limit [Count: " + std::to_string(limit_) + ", Offset: " + std::to_string(offset_) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }

    private:
        const AbstractPlanNode* child_;
        int32_t limit_;
        int32_t offset_;
    };

} // namespace tetodb