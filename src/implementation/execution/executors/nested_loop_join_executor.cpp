// nested_loop_join_executor.cpp

#include "execution/executors/nested_loop_join_executor.h"

namespace tetodb {

    NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutionContext* exec_ctx,
        const NestedLoopJoinPlanNode* plan,
        std::unique_ptr<AbstractExecutor> left_child,
        std::unique_ptr<AbstractExecutor> right_child)
        : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_child)), right_executor_(std::move(right_child)) {
    }

    void NestedLoopJoinExecutor::Init() {
        left_executor_->Init();
        right_executor_->Init();

        RID dummy_rid;
        has_left_tuple_ = left_executor_->Next(&left_tuple_, &dummy_rid);
    }

    bool NestedLoopJoinExecutor::Next(Tuple* tuple, RID* rid) {
        RID right_rid;
        Tuple right_tuple;

        while (has_left_tuple_) {

            while (right_executor_->Next(&right_tuple, &right_rid)) {

                // Evaluate using the plan's predicate!
                Value result = plan_->Predicate()->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                    &right_tuple, right_executor_->GetOutputSchema());

                if (result.GetTypeId() != TypeId::INVALID && result.GetAsBoolean()) {

                    std::vector<Value> combined_values;

                    const Schema* left_schema = left_executor_->GetOutputSchema();
                    for (uint32_t i = 0; i < left_schema->GetColumnCount(); i++) {
                        combined_values.push_back(left_tuple_.GetValue(left_schema, i));
                    }

                    const Schema* right_schema = right_executor_->GetOutputSchema();
                    for (uint32_t i = 0; i < right_schema->GetColumnCount(); i++) {
                        combined_values.push_back(right_tuple.GetValue(right_schema, i));
                    }

                    *tuple = Tuple(combined_values, plan_->OutputSchema());

                    return true;
                }
            }

            RID dummy_left_rid;
            has_left_tuple_ = left_executor_->Next(&left_tuple_, &dummy_left_rid);

            if (has_left_tuple_) {
                right_executor_->Init();
            }
        }

        return false;
    }

} // namespace tetodb