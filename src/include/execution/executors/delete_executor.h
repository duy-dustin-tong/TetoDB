// delete_executor.h

#pragma once

#include <memory>
#include <vector>
#include "execution/executors/abstract_executor.h"
#include "storage/table/tuple.h"
#include "catalog/catalog.h"
#include "execution/plans/delete_plan.h" // <-- The new blueprint!

namespace tetodb {

    class DeleteExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION: Paid the technical debt!
        DeleteExecutor(ExecutionContext* exec_ctx,
            const DeletePlanNode* plan,
            std::unique_ptr<AbstractExecutor> child_executor);

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;
        const Schema* GetOutputSchema() override;

    private:
        const DeletePlanNode* plan_; // <-- The Blueprint!
        std::unique_ptr<AbstractExecutor> child_executor_;
        TableMetadata* table_info_;
        std::vector<IndexMetadata*> table_indexes_;
    };

} // namespace tetodb