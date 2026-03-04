// distinct_executor.cpp

#include "execution/executors/distinct_executor.h"

namespace tetodb {

DistinctExecutor::DistinctExecutor(ExecutionContext *exec_ctx,
                                   const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> child)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {}

void DistinctExecutor::Init() {
  child_->Init();
  seen_tuples_.clear();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  while (child_->Next(tuple, rid)) {
    DistinctKey key;
    for (uint32_t i = 0; i < plan_->OutputSchema()->GetColumnCount(); i++) {
      key.values_.push_back(tuple->GetValue(plan_->OutputSchema(), i));
    }

    if (seen_tuples_.find(key) == seen_tuples_.end()) {
      seen_tuples_.insert(key);
      return true;
    }
  }
  return false;
}

} // namespace tetodb
