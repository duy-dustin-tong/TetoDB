// sort_executor.cpp

#include "execution/executors/sort_executor.h"
#include <algorithm>

namespace tetodb {

    SortExecutor::SortExecutor(ExecutionContext* exec_ctx,
        const SortPlanNode* plan,
        std::unique_ptr<AbstractExecutor> child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
    }

    void SortExecutor::Init() {
        child_executor_->Init();
        sorted_tuples_.clear();
        cursor_ = 0;

        Tuple child_tuple;
        RID child_rid;

        // 1. Materialize: Pull EVERY row from the child into RAM
        while (child_executor_->Next(&child_tuple, &child_rid)) {
            sorted_tuples_.push_back(child_tuple);
        }

        const Schema* schema = child_executor_->GetOutputSchema();
        const auto& order_bys = plan_->GetOrderBys();

        // 2. Sort: Use std::sort with a custom comparison lambda
        std::sort(sorted_tuples_.begin(), sorted_tuples_.end(),
            [schema, &order_bys](const Tuple& a, const Tuple& b) {

                for (const auto& order_pair : order_bys) {
                    OrderByType type = order_pair.first;
                    const AbstractExpression* expr = order_pair.second;

                    Value val_a = expr->Evaluate(&a, schema);
                    Value val_b = expr->Evaluate(&b, schema);

                    // If equal, continue to the next tie-breaker column
                    if (val_a.CompareEquals(val_b)) {
                        continue;
                    }

                    // Otherwise, we found a difference! Sort ASC or DESC
                    if (type == OrderByType::DESC) {
                        return val_b.CompareLessThan(val_a);
                    }
                    else {
                        return val_a.CompareLessThan(val_b); // ASC is default
                    }
                }

                return false;
            }
        );
    }

    bool SortExecutor::Next(Tuple* tuple, RID* rid) {
        // 3. Emit: Hand the perfectly sorted tuples up the pipeline one by one
        if (cursor_ < sorted_tuples_.size()) {
            *tuple = sorted_tuples_[cursor_];
            // Sort typically breaks original physical RIDs since it creates new copies in memory.
            // But we will just pass up a dummy RID since parents of Sort (like Limit or Projection) don't need physical disk locations.
            *rid = RID();
            cursor_++;
            return true;
        }
        return false;
    }

    const Schema* SortExecutor::GetOutputSchema() {
        return plan_->OutputSchema();
    }

} // namespace tetodb