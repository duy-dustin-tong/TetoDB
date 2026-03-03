// sort_executor.h

#pragma once

#include <memory>
#include <vector>
#include "execution/executors/abstract_executor.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace tetodb {

    class SortExecutor : public AbstractExecutor {
    public:
        SortExecutor(ExecutionContext* exec_ctx,
            const SortPlanNode* plan,
            std::unique_ptr<AbstractExecutor> child_executor);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override;

    private:
        const SortPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_executor_;

        // The materialization buffer for sorting
        std::vector<Tuple> sorted_tuples_;
        size_t cursor_{ 0 };
    };

} // namespace tetodb