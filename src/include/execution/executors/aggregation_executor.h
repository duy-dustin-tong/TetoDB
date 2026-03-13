// aggregation_executor.h

#pragma once

#include <memory>
#include <vector>
#include <unordered_map>
#include <string>

#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "type/value.h"
#include "execution/plans/aggregation_plan.h" // Gets the Enum from here now!

namespace tetodb {

    struct AggregateKey {
        std::vector<Value> group_bys_;

        bool operator==(const AggregateKey& other) const {
            if (group_bys_.size() != other.group_bys_.size()) return false;
            for (size_t i = 0; i < group_bys_.size(); i++) {
                if (!group_bys_[i].CompareEquals(other.group_bys_[i])) return false;
            }
            return true;
        }
    };

    struct AggregateValue {
        std::vector<Value> aggregates_;
        std::vector<int32_t> counts_;
        std::vector<std::vector<Value>> history_;
    };

    struct AggregateKeyHash {
        std::size_t operator()(const AggregateKey& key) const {
            std::size_t hash = 0;
            for (const auto& val : key.group_bys_) {
                hash ^= val.Hash() + 0x9e3779b9 + (hash << 6) + (hash >> 2);
            }
            return hash;
        }
    };

    class AggregationExecutor : public AbstractExecutor {
    public:
        // PURE DECLARATION (Ends with semicolon)
        AggregationExecutor(ExecutionContext* exec_ctx,
            const AggregationPlanNode* plan,
            std::unique_ptr<AbstractExecutor> child);

        void Init() override;

        bool Next(Tuple* tuple, RID* rid) override;

        const Schema* GetOutputSchema() override { return plan_->OutputSchema(); }

    private:
        void InsertCombine(const AggregateKey& agg_key, const AggregateValue& agg_val);
        Value CombineAggregateValues(AggregationType agg_type, const Value& running_val, const Value& new_val);

        const AggregationPlanNode* plan_;
        std::unique_ptr<AbstractExecutor> child_;

        std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash> ht_;
        std::unordered_map<AggregateKey, AggregateValue, AggregateKeyHash>::iterator ht_iterator_;
    };

} // namespace tetodb