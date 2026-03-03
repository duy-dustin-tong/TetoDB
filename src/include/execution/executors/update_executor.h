// update_executor.h

#pragma once

#include <memory>
#include <vector>
#include "execution/executors/abstract_executor.h"
#include "execution/plans/update_plan.h"
#include "storage/table/tuple.h"
#include "catalog/catalog.h"

namespace tetodb {

    class UpdateExecutor : public AbstractExecutor {
    public:
        UpdateExecutor(ExecutionContext* exec_ctx,
            const UpdatePlanNode* plan,
            std::unique_ptr<AbstractExecutor> child_executor);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override;

    private:
        const UpdatePlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_executor_;
        TableMetadata* table_info_;
        std::vector<IndexMetadata*> table_indexes_;

        // --- NEW: Materialization buffers to prevent the Halloween Problem ---
        std::vector<RID> target_rids_;
        size_t cursor_{ 0 };
    };

} // namespace tetodb