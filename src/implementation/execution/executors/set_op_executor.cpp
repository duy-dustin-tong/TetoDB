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

  memory_map_.clear();
  emitted_set_.clear();

  SetOpType set_type = plan_->GetSetOpType();

  if (set_type == SetOpType::INTERSECT) {
    // Build hash map (multiset) from left child
    Tuple tuple;
    RID rid;
    while (left_child_->Next(&tuple, &rid)) {
      memory_map_[MakeKey(&tuple, left_child_->GetOutputSchema())]++;
    }
    // We will stream the right child and probe memory_map_
  } else if (set_type == SetOpType::EXCEPT) {
    // Build hash map (multiset) from right child
    Tuple tuple;
    RID rid;
    while (right_child_->Next(&tuple, &rid)) {
      memory_map_[MakeKey(&tuple, right_child_->GetOutputSchema())]++;
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
    // Stream right child, look for matches in memory_map_
    while (right_child_->Next(tuple, rid)) {
      auto key = MakeKey(tuple, right_child_->GetOutputSchema());

      auto it = memory_map_.find(key);
      if (it != memory_map_.end() && it->second > 0) {
        if (!plan_->IsAll()) {
          // INTERSECT DISTINCT: Emit once, then zero the count to never emit
          // again
          it->second = 0;
          return true;
        } else {
          // INTERSECT ALL: Bag intersection (MIN count)
          // Decrement the left-side token matching this right-side row
          it->second--;
          return true;
        }
      }
    }
    return false;

  } else if (set_type == SetOpType::EXCEPT) {
    // Stream left child, exclude if in memory_map_ (which is built from right)
    while (left_child_->Next(tuple, rid)) {
      auto key = MakeKey(tuple, left_child_->GetOutputSchema());

      auto it = memory_map_.find(key);
      if (it != memory_map_.end() && it->second > 0) {
        // MATCH FOUND IN RIGHT CHILD:
        // Consume one right-side token and DO NOT emit this row (exclude it)
        it->second--;
        continue;
      }

      // NO MATCH FOUND (OR RIGHT SIDE COUNTS EXHAUSTED):
      if (plan_->IsAll()) {
        // EXCEPT ALL: Emit the remaining tokens freely
        return true;
      } else {
        // EXCEPT DISTINCT: Deduplicate the final result set
        if (emitted_set_.insert(key).second) {
          return true;
        }
      }
    }
    return false;
  }

  return false;
}

} // namespace tetodb
