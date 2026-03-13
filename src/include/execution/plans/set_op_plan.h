// set_op_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "parser/ast.h" // For SetOpType

namespace tetodb {

class SetOpPlanNode : public AbstractPlanNode {
public:
  SetOpPlanNode(const Schema *output_schema, const AbstractPlanNode *left_child,
                const AbstractPlanNode *right_child, SetOpType set_op_type,
                bool is_all)
      : AbstractPlanNode(output_schema, PlanType::SetOp),
        left_child_(left_child), right_child_(right_child),
        set_op_type_(set_op_type), is_all_(is_all) {}

  std::string ToString() const override {
    std::string op = "";
    if (set_op_type_ == SetOpType::UNION)
      op = "UNION";
    else if (set_op_type_ == SetOpType::INTERSECT)
      op = "INTERSECT";
    else
      op = "EXCEPT";
    return op + (is_all_ ? " ALL" : "");
  }

  std::vector<const AbstractPlanNode *> GetChildren() const override {
    return {left_child_, right_child_};
  }

  SetOpType GetSetOpType() const { return set_op_type_; }
  bool IsAll() const { return is_all_; }

  const AbstractPlanNode *GetLeftPlan() const { return left_child_; }
  const AbstractPlanNode *GetRightPlan() const { return right_child_; }

private:
  const AbstractPlanNode *left_child_;
  const AbstractPlanNode *right_child_;
  SetOpType set_op_type_;
  bool is_all_;
};

} // namespace tetodb
