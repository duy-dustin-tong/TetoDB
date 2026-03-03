// topn_executor.h

#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <algorithm>

#include "execution/executors/abstract_executor.h"
#include "execution/plans/topn_plan.h"

namespace tetodb {

    class TopNExecutor : public AbstractExecutor {
    public:
        TopNExecutor(ExecutionContext* exec_ctx, const TopNPlanNode* plan, std::unique_ptr<AbstractExecutor> child_executor)
            : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
        }

        const Schema* GetOutputSchema() override;

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;

    private:
        const TopNPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_executor_;

        std::vector<Tuple> top_entries_;
        size_t cursor_{ 0 };
    };

} // namespace tetodb