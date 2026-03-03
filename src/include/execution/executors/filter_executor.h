// filter_executor.h

#pragma once

#include <memory>
#include "execution/executors/abstract_executor.h"
#include "execution/plans/filter_plan.h"
#include "storage/table/tuple.h"

namespace tetodb {

    class FilterExecutor : public AbstractExecutor {
    public:
        FilterExecutor(ExecutionContext* exec_ctx,
            const FilterPlanNode* plan,
            std::unique_ptr<AbstractExecutor> child);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override;

    private:
        const FilterPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_;
    };

} // namespace tetodb