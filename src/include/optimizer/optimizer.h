// optimizer.h

#pragma once

#include <memory>
#include <vector>
#include "execution/plans/abstract_plan.h"
#include "catalog/catalog.h"

namespace tetodb {

    class Optimizer {
    public:
        explicit Optimizer(Catalog* catalog) : catalog_(catalog) {}

        const AbstractPlanNode* Optimize(const AbstractPlanNode* plan);

    private:
        const AbstractPlanNode* OptimizeCustomRules(const AbstractPlanNode* plan);

        // --- THE OPTIMIZER RULES ---
        const AbstractPlanNode* OptimizeNLJToHashJoin(const AbstractPlanNode* plan);
        const AbstractPlanNode* OptimizeSeqScanAsIndexScan(const AbstractPlanNode* plan);
        const AbstractPlanNode* OptimizeSortLimitAsTopN(const AbstractPlanNode* plan);

        Catalog* catalog_;
        std::vector<std::unique_ptr<AbstractPlanNode>> optimized_nodes_;
    };

} // namespace tetodb