// index_scan_executor.h

#pragma once

#include <vector>
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "index/b_plus_tree.h" 

namespace tetodb {

    class IndexScanExecutor : public AbstractExecutor {
    public:
        IndexScanExecutor(ExecutionContext* exec_ctx, const IndexScanPlanNode* plan);

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;
        const Schema* GetOutputSchema() override;

    private:
        const IndexScanPlanNode* plan_;
        TableMetadata* table_metadata_;

        // Results from the B+ Tree
        std::vector<RID> result_rids_;
        uint32_t cursor_{ 0 };
    };

}  // namespace tetodb