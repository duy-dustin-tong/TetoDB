// hash_join_executor.cpp

#include "execution/executors/hash_join_executor.h"

namespace tetodb {

    // Constructor updated to match the new header!
    HashJoinExecutor::HashJoinExecutor(ExecutionContext* exec_ctx,
        const HashJoinPlanNode* plan,
        std::unique_ptr<AbstractExecutor> left_child,
        std::unique_ptr<AbstractExecutor> right_child)
        : AbstractExecutor(exec_ctx),
        plan_(plan),
        left_child_(std::move(left_child)),
        right_child_(std::move(right_child)) {
    }

    void HashJoinExecutor::Init() {
        left_child_->Init();
        right_child_->Init();

        ht_.clear();
        has_right_tuple_ = false;
        match_index_ = 0;

        Tuple left_tuple;
        RID left_rid;
        const Schema* left_schema = left_child_->GetOutputSchema();

        while (left_child_->Next(&left_tuple, &left_rid)) {
            // Extract using the plan's expression!
            Value join_key_val = plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_schema);
            HashJoinKey key{ join_key_val };

            ht_[key].push_back(left_tuple);
        }

        RID right_rid;
        has_right_tuple_ = right_child_->Next(&right_tuple_, &right_rid);
    }

    bool HashJoinExecutor::Next(Tuple* tuple, RID* rid) {
        const Schema* left_schema = left_child_->GetOutputSchema();
        const Schema* right_schema = right_child_->GetOutputSchema();

        while (has_right_tuple_) {

            // Extract using the plan's expression!
            Value probe_key_val = plan_->RightJoinKeyExpression()->Evaluate(&right_tuple_, right_schema);
            HashJoinKey key{ probe_key_val };

            if (ht_.find(key) != ht_.end()) {
                const std::vector<Tuple>& left_tuples = ht_[key];

                if (match_index_ < left_tuples.size()) {

                    Tuple matching_left_tuple = left_tuples[match_index_];
                    match_index_++;

                    std::vector<Value> combined_values;

                    for (uint32_t i = 0; i < left_schema->GetColumnCount(); i++) {
                        combined_values.push_back(matching_left_tuple.GetValue(left_schema, i));
                    }
                    for (uint32_t i = 0; i < right_schema->GetColumnCount(); i++) {
                        combined_values.push_back(right_tuple_.GetValue(right_schema, i));
                    }

                    *tuple = Tuple(combined_values, plan_->OutputSchema());
                    *rid = RID();

                    return true;
                }
            }

            RID right_rid;
            has_right_tuple_ = right_child_->Next(&right_tuple_, &right_rid);
            match_index_ = 0;
        }

        return false;
    }

} // namespace tetodb