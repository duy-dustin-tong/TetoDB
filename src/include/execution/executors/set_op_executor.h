// set_op_executor.h

#pragma once

#include <memory>
#include <unordered_map>
#include <unordered_set>

#include "execution/executors/abstract_executor.h"
#include "execution/executors/distinct_executor.h" // For DistinctKey
#include "execution/plans/set_op_plan.h"

namespace tetodb {

class SetOpExecutor : public AbstractExecutor {
public:
  SetOpExecutor(ExecutionContext *exec_ctx, const SetOpPlanNode *plan,
                std::unique_ptr<AbstractExecutor> left_child,
                std::unique_ptr<AbstractExecutor> right_child);

  void Init() override;
  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

private:
  const SetOpPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;

  bool initial_init_{false};
  bool left_exhausted_{false};

  // Used for INTERSECT and EXCEPT (multiset counting)
  std::unordered_map<DistinctKey, uint32_t, DistinctKeyHash> memory_map_;
  // Used for deduplication (UNION without ALL, or left side of EXCEPT)
  std::unordered_set<DistinctKey, DistinctKeyHash> emitted_set_;

  DistinctKey MakeKey(const Tuple *tuple, const Schema *schema);
};

} // namespace tetodb
