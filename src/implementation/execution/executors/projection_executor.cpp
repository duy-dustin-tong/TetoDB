// projection_executor.cpp

#include "execution/executors/projection_executor.h"

namespace tetodb {
    ProjectionExecutor::ProjectionExecutor(ExecutionContext* exec_ctx,
        const ProjectionPlanNode* plan,
        std::unique_ptr<AbstractExecutor> child)
        : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {
    }

    void ProjectionExecutor::Init() {
        child_->Init();
    }

    bool ProjectionExecutor::Next(Tuple* tuple, RID* rid) {
        Tuple child_tuple;
        RID child_rid;

        if (child_->Next(&child_tuple, &child_rid)) {

            std::vector<Value> projected_values;
            projected_values.reserve(plan_->GetExpressions().size());
            const Schema* child_schema = child_->GetOutputSchema();

            // --- UPDATED: Pass the parameter context into the evaluation! ---
            for (const auto* expr : plan_->GetExpressions()) {
                projected_values.push_back(expr->Evaluate(&child_tuple, child_schema, exec_ctx_->GetParams()));
            }

            *tuple = Tuple(projected_values, plan_->OutputSchema());
            *rid = child_rid;

            return true;
        }

        return false;
    }

} // namespace tetodb