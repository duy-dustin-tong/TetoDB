// limit_executor.cpp

#include "execution/executors/limit_executor.h"

namespace tetodb {

    LimitExecutor::LimitExecutor(ExecutionContext* exec_ctx,
        const LimitPlanNode* plan,
        std::unique_ptr<AbstractExecutor> child)
        : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {
    }

    void LimitExecutor::Init() {
        child_->Init();
        cursor_ = 0;
    }

    bool LimitExecutor::Next(Tuple* tuple, RID* rid) {
        while (child_->Next(tuple, rid)) {

            // 1. Skip rows until we clear the OFFSET
            if (cursor_ < plan_->GetOffset()) {
                cursor_++;
                continue;
            }

            // 2. Stop executing entirely if we hit the LIMIT
            if (plan_->GetLimit() != -1 && (cursor_ - plan_->GetOffset()) >= plan_->GetLimit()) {
                return false;
            }

            // 3. Otherwise, pass the row up!
            cursor_++;
            return true;
        }
        return false;
    }

    const Schema* LimitExecutor::GetOutputSchema() {
        return plan_->OutputSchema();
    }

} // namespace tetodb