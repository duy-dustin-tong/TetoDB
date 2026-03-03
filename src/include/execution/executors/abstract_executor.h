// abstract_executor.h

#pragma once

#include "execution/execution_context.h"
#include "catalog/schema.h"

namespace tetodb {

    /**
     * The AbstractExecutor implements the Volcano (Iterator) Model.
     * 1. Init(): Initialize the operator (e.g., open file, set pointers to 0).
     * 2. Next(): Emit the next tuple. Returns true if a tuple was emitted, false if finished.
     */
    class AbstractExecutor {
    public:
        explicit AbstractExecutor(ExecutionContext* exec_ctx) : exec_ctx_(exec_ctx) {}
        virtual ~AbstractExecutor() = default;

        /**
         * Initialize the executor.
         * This is called ONCE before execution begins.
         */
        virtual void Init() = 0;

        /**
         * Yield the next tuple from this operator.
         * @param[out] tuple The next tuple produced by this operator
         * @param[out] rid The RID of the produced tuple (useful for Deletes/Updates)
         * @return true if a tuple was produced, false if there are no more tuples
         */
        virtual bool Next(Tuple* tuple, RID* rid) = 0;

        /** @return The schema of the tuples produced by this executor */
        virtual const Schema* GetOutputSchema() = 0;

        inline ExecutionContext* GetExecutorContext() { return exec_ctx_; }

    protected:
        ExecutionContext* exec_ctx_;
    };

} // namespace tetodb