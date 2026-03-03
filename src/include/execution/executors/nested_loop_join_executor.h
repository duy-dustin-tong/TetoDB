// nested_loop_join_executor.h

#pragma once

#include <memory>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "storage/table/tuple.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h" 
#include "execution/plans/nested_loop_join_plan.h"

namespace tetodb {

    class NestedLoopJoinExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION
        NestedLoopJoinExecutor(ExecutionContext* exec_ctx,
            const NestedLoopJoinPlanNode* plan,
            std::unique_ptr<AbstractExecutor> left_child,
            std::unique_ptr<AbstractExecutor> right_child);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override { return plan_->OutputSchema(); }

    private:
        const NestedLoopJoinPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> left_executor_;
        std::unique_ptr<AbstractExecutor> right_executor_;

        Tuple left_tuple_;
        bool has_left_tuple_{ false };
    };

} // namespace tetodb