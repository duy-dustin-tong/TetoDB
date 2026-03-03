// execution_engine.h

#pragma once

#include <memory>
#include <stdexcept>

#include "execution/execution_context.h"
#include "execution/plans/abstract_plan.h"
#include "execution/executors/abstract_executor.h"

namespace tetodb {

    class ExecutionEngine {
    public:
        // Now it ONLY builds the tree and hands it back. No more vectors!
        static std::unique_ptr<AbstractExecutor> CreateExecutor(const AbstractPlanNode* plan, ExecutionContext* exec_ctx);
    };

} // namespace tetodb