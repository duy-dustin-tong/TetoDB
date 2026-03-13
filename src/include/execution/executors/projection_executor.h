// projection_executor.h

#pragma once

#include <memory>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/projection_plan.h" 

namespace tetodb {

    class ProjectionExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION
        ProjectionExecutor(ExecutionContext* exec_ctx,
            const ProjectionPlanNode* plan,
            std::unique_ptr<AbstractExecutor> child);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override { return plan_->OutputSchema(); }

    private:
        const ProjectionPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_;
    };

} // namespace tetodb