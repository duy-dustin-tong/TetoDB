// insert_executor.h

#pragma once

#include <vector>
#include "execution/executors/abstract_executor.h"
#include "storage/table/tuple.h"
#include "catalog/catalog.h" 
#include "execution/plans/insert_plan.h" // <-- The Blueprint

namespace tetodb {

    class InsertExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION: Paid the technical debt!
        InsertExecutor(ExecutionContext* exec_ctx, const InsertPlanNode* plan);

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;
        const Schema* GetOutputSchema() override;

    private:
        const InsertPlanNode* plan_; // The Blueprint!
        TableMetadata* table_info_;
        std::vector<IndexMetadata*> table_indexes_;
        size_t cursor_;
    };

} // namespace tetodb