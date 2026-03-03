// hash_join_executor.h

#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"
#include "execution/plans/hash_join_plan.h"

namespace tetodb {

    struct HashJoinKey {
        Value val_;
        bool operator==(const HashJoinKey& other) const {
            return val_.CompareEquals(other.val_);
        }
    };

    struct HashJoinKeyHash {
        std::size_t operator()(const HashJoinKey& key) const {
            return key.val_.Hash();
        }
    };

    class HashJoinExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION
        HashJoinExecutor(ExecutionContext* exec_ctx,
            const HashJoinPlanNode* plan,
            std::unique_ptr<AbstractExecutor> left_child,
            std::unique_ptr<AbstractExecutor> right_child);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override { return plan_->OutputSchema(); }

    private:
        const HashJoinPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> left_child_;
        std::unique_ptr<AbstractExecutor> right_child_;

        std::unordered_map<HashJoinKey, std::vector<Tuple>, HashJoinKeyHash> ht_;
        Tuple right_tuple_;
        bool has_right_tuple_{ false };
        size_t match_index_{ 0 };
    };

} // namespace tetodb