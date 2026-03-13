// distinct_executor.h

#pragma once

#include <memory>
#include <unordered_set>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/distinct_plan.h"
#include "type/value.h"

namespace tetodb {

struct DistinctKey {
  std::vector<Value> values_;

  bool operator==(const DistinctKey &other) const {
    if (values_.size() != other.values_.size())
      return false;
    for (size_t i = 0; i < values_.size(); i++) {
      if (!values_[i].CompareEquals(other.values_[i]))
        return false;
    }
    return true;
  }
};

struct DistinctKeyHash {
  std::size_t operator()(const DistinctKey &key) const {
    std::size_t hash = 0;
    for (const auto &val : key.values_) {
      hash ^= val.Hash() + 0x9e3779b9 + (hash << 6) + (hash >> 2);
    }
    return hash;
  }
};

class DistinctExecutor : public AbstractExecutor {
public:
  DistinctExecutor(ExecutionContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> child);

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

private:
  const DistinctPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_;
  std::unordered_set<DistinctKey, DistinctKeyHash> seen_tuples_;
};

} // namespace tetodb
