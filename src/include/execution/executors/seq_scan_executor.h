// seq_scan_executor.h

#pragma once

#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h" 
#include "storage/table/table_iterator.h"

namespace tetodb {

    class SeqScanExecutor : public AbstractExecutor {
    public:
        // Constructor takes the PLAN now
        SeqScanExecutor(ExecutionContext* exec_ctx, const SeqScanPlanNode* plan);

        void Init() override;
        bool Next(Tuple* tuple, RID* rid) override;
        const Schema* GetOutputSchema() override;

    private:
        const SeqScanPlanNode* plan_; // Store plan instead of table_name_
        TableMetadata* metadata_;
        // Use unique_ptr for iterator to allow lazy initialization
        std::unique_ptr<TableIterator> iter_;
    };

} // namespace tetodb