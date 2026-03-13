// filter_executor.cpp

#include "execution/executors/filter_executor.h"

namespace tetodb {

    FilterExecutor::FilterExecutor(ExecutionContext* exec_ctx,
        const FilterPlanNode* plan,
        std::unique_ptr<AbstractExecutor> child)
        : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {
    }

    void FilterExecutor::Init() {
        child_->Init();
    }

    bool FilterExecutor::Next(Tuple* tuple, RID* rid) {
        // Keep pulling rows from the child until one passes the filter, or we run out of rows
        while (child_->Next(tuple, rid)) {

            // Evaluate the WHERE clause against the current tuple
            Value result = plan_->GetPredicate()->Evaluate(tuple, child_->GetOutputSchema(), exec_ctx_->GetParams());

            // If the predicate evaluates to true, pass this tuple up to the parent!
            if (result.GetTypeId() != TypeId::INVALID && result.GetAsBoolean()) {
                return true;
            }

            // Otherwise, loop around and pull the next tuple from the child.
        }

        // The child ran out of rows.
        return false;
    }

    const Schema* FilterExecutor::GetOutputSchema() {
        return plan_->OutputSchema();
    }

} // namespace tetodb