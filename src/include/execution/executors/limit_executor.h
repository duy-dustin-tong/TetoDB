// limit_executor.h

#pragma once

#include <memory>
#include "execution/executors/abstract_executor.h"
#include "execution/plans/limit_plan.h"
#include "storage/table/tuple.h"

namespace tetodb {

    class LimitExecutor : public AbstractExecutor {
    public:
        LimitExecutor(ExecutionContext* exec_ctx,
            const LimitPlanNode* plan,
            std::unique_ptr<AbstractExecutor> child);

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;
        const Schema* GetOutputSchema() override;

    private:
        const LimitPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_;
        size_t cursor_{ 0 };
    };

} // namespace tetodb