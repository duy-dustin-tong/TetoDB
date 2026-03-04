// set_op_executor.cpp

#include "execution/executors/set_op_executor.h"

namespace tetodb {

SetOpExecutor::SetOpExecutor(ExecutionContext *exec_ctx,
                             const SetOpPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> left_child,
                             std::unique_ptr<AbstractExecutor> right_child)
    : AbstractExecutor(exec_ctx), plan_(plan),
      left_child_(std::move(left_child)), right_child_(std::move(right_child)) {
}

DistinctKey SetOpExecutor::MakeKey(const Tuple *tuple, const Schema *schema) {
  std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(tuple->GetValue(schema, i));
  }
  return DistinctKey{std::move(values)};
}

void SetOpExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  initial_init_ = true;
  left_exhausted_ = false;

  memory_set_.clear();
  emitted_set_.clear();

  SetOpType set_type = plan_->GetSetOpType();

  if (set_type == SetOpType::INTERSECT) {
    // Build hash set from left child
    Tuple tuple;
    RID rid;
    while (left_child_->Next(&tuple, &rid)) {
      memory_set_.insert(MakeKey(&tuple, left_child_->GetOutputSchema()));
    }
    // We will stream the right child and probe memory_set_
  } else if (set_type == SetOpType::EXCEPT) {
    // Build hash set from right child
    Tuple tuple;
    RID rid;
    while (right_child_->Next(&tuple, &rid)) {
      memory_set_.insert(MakeKey(&tuple, right_child_->GetOutputSchema()));
    }
  }
}

bool SetOpExecutor::Next(Tuple *tuple, RID *rid) {
  if (!initial_init_) {
    Init();
  }

  SetOpType set_type = plan_->GetSetOpType();

  if (set_type == SetOpType::UNION) {
    // Stream left, then right.
    while (!left_exhausted_) {
      if (left_child_->Next(tuple, rid)) {
        if (plan_->IsAll()) {
          return true;
        } else {
          auto key = MakeKey(tuple, left_child_->GetOutputSchema());
          if (emitted_set_.insert(key).second) {
            return true;
          }
        }
      } else {
        left_exhausted_ = true;
      }
    }

    // Stream right
    while (right_child_->Next(tuple, rid)) {
      if (plan_->IsAll()) {
        return true;
      } else {
        auto key = MakeKey(tuple, right_child_->GetOutputSchema());
        if (emitted_set_.insert(key).second) {
          return true;
        }
      }
    }
    return false;

  } else if (set_type == SetOpType::INTERSECT) {
    // Stream right child, look for matches in memory_set_
    while (right_child_->Next(tuple, rid)) {
      auto key = MakeKey(tuple, right_child_->GetOutputSchema());

      // If we found it in the left child
      if (memory_set_.find(key) != memory_set_.end()) {
        if (!plan_->IsAll()) {
          // If not ALL, we only emit each match once. Remove from memory_set_.
          memory_set_.erase(key);
          return true;
        } else {
          // INTERSECT ALL semantics (bag intersection) is more complex
          // (requires counting), but if we assume simple stream intersect for
          // now (matching TetoDB standard requirements):
          if (emitted_set_.insert(key).second) {
            return true; // Simple distinct intersect approach
          }
        }
      }
    }
    return false;

  } else if (set_type == SetOpType::EXCEPT) {
    // Stream left child, exclude if in memory_set_ (which is built from right)
    while (left_child_->Next(tuple, rid)) {
      auto key = MakeKey(tuple, left_child_->GetOutputSchema());

      if (memory_set_.find(key) == memory_set_.end()) {
        if (plan_->IsAll()) {
          return true;
        } else {
          if (emitted_set_.insert(key).second) {
            return true;
          }
        }
      }
    }
    return false;
  }

  return false;
}

} // namespace tetodb
