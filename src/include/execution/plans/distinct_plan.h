// distinct_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include <vector>


namespace tetodb {

class DistinctPlanNode : public AbstractPlanNode {
public:
  DistinctPlanNode(const Schema *output_schema, const AbstractPlanNode *child)
      : AbstractPlanNode(output_schema, PlanType::Distinct), child_(child) {}

  inline const AbstractPlanNode *GetChildPlan() const { return child_; }

  std::string ToString() const override { return "Distinct"; }

  std::vector<const AbstractPlanNode *> GetChildren() const override {
    return {child_};
  }

private:
  const AbstractPlanNode *child_;
};

} // namespace tetodb
