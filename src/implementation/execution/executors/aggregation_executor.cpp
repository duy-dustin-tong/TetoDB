// aggregation_executor.cpp

#include "execution/executors/aggregation_executor.h"
#include <algorithm> // Required for std::sort (Median)

namespace tetodb {

    AggregationExecutor::AggregationExecutor(ExecutionContext* exec_ctx,
        const AggregationPlanNode* plan,
        std::unique_ptr<AbstractExecutor> child)
        : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)) {
    }

    void AggregationExecutor::Init() {
        child_->Init();
        ht_.clear();

        Tuple child_tuple;
        RID child_rid;
        const Schema* child_schema = child_->GetOutputSchema();

        while (child_->Next(&child_tuple, &child_rid)) {
            AggregateKey agg_key;
            for (const auto* expr : plan_->GetGroupBys()) {
                agg_key.group_bys_.push_back(expr->Evaluate(&child_tuple, child_schema));
            }

            AggregateValue agg_val;
            for (const auto* expr : plan_->GetAggregates()) {
                agg_val.aggregates_.push_back(expr->Evaluate(&child_tuple, child_schema));
            }

            InsertCombine(agg_key, agg_val);
        }

        ht_iterator_ = ht_.begin();
    }

    bool AggregationExecutor::Next(Tuple* tuple, RID* rid) {
        if (ht_iterator_ == ht_.end()) {
            return false;
        }

        std::vector<Value> final_values;
        const auto& agg_types = plan_->GetAggregateTypes();

        // 1. Output the GROUP BY columns
        for (const auto& val : ht_iterator_->first.group_bys_) {
            final_values.push_back(val);
        }

        // 2. Output the AGGREGATE columns
        for (size_t i = 0; i < agg_types.size(); i++) {
            if (agg_types[i] == AggregationType::AVERAGE) {
                double sum = ht_iterator_->second.aggregates_[i].CastAsDouble();
                int32_t count = ht_iterator_->second.counts_[i];
                double avg = (count == 0) ? 0.0 : (sum / count);
                final_values.push_back(Value(TypeId::DECIMAL, avg));
            }
            else if (agg_types[i] == AggregationType::MEDIAN) {
                auto hist = ht_iterator_->second.history_[i];
                if (hist.empty()) {
                    final_values.push_back(Value(TypeId::DECIMAL, 0.0));
                }
                else {
                    std::sort(hist.begin(), hist.end(), [](const Value& a, const Value& b) {
                        return a.CompareLessThan(b);
                        });

                    size_t n = hist.size();
                    if (n % 2 == 0) {
                        double mid1 = hist[n / 2 - 1].CastAsDouble();
                        double mid2 = hist[n / 2].CastAsDouble();
                        final_values.push_back(Value(TypeId::DECIMAL, (mid1 + mid2) / 2.0));
                    }
                    else {
                        final_values.push_back(Value(TypeId::DECIMAL, hist[n / 2].CastAsDouble()));
                    }
                }
            }
            else {
                // Standard distributive aggregates (SUM, COUNT, MIN, MAX)
                final_values.push_back(ht_iterator_->second.aggregates_[i]);
            }
        }

        *tuple = Tuple(final_values, plan_->OutputSchema());
        *rid = RID(); // Aggregations generate brand new virtual rows

        ++ht_iterator_;
        return true;
    }


    void AggregationExecutor::InsertCombine(const AggregateKey& agg_key, const AggregateValue& agg_val) {
        const auto& agg_types = plan_->GetAggregateTypes();

        if (ht_.find(agg_key) == ht_.end()) {
            AggregateValue initial_val;
            initial_val.aggregates_.resize(agg_types.size());
            initial_val.counts_.resize(agg_types.size(), 0);
            initial_val.history_.resize(agg_types.size());

            for (size_t i = 0; i < agg_types.size(); i++) {
                if (agg_types[i] == AggregationType::COUNT_STAR) {
                    initial_val.aggregates_[i] = Value(TypeId::INTEGER, 1);
                }
                else if (agg_types[i] == AggregationType::AVERAGE) {
                    initial_val.aggregates_[i] = agg_val.aggregates_[i];
                    initial_val.counts_[i] = 1;
                }
                else if (agg_types[i] == AggregationType::MEDIAN) {
                    initial_val.history_[i].push_back(agg_val.aggregates_[i]);
                }
                else {
                    initial_val.aggregates_[i] = agg_val.aggregates_[i];
                }
            }
            ht_[agg_key] = initial_val;
            return;
        }

        // Combine existing values
        for (size_t i = 0; i < agg_types.size(); i++) {
            if (agg_types[i] == AggregationType::AVERAGE) {
                ht_[agg_key].aggregates_[i] = ht_[agg_key].aggregates_[i].Add(agg_val.aggregates_[i]);
                ht_[agg_key].counts_[i]++;
            }
            else if (agg_types[i] == AggregationType::MEDIAN) {
                ht_[agg_key].history_[i].push_back(agg_val.aggregates_[i]);
            }
            else {
                ht_[agg_key].aggregates_[i] = CombineAggregateValues(agg_types[i], ht_[agg_key].aggregates_[i], agg_val.aggregates_[i]);
            }
        }
    }

    Value AggregationExecutor::CombineAggregateValues(AggregationType agg_type, const Value& running_val, const Value& new_val) {
        switch (agg_type) {
        case AggregationType::COUNT_STAR:
            return running_val.Add(Value(TypeId::INTEGER, 1));
        case AggregationType::SUM:
            return running_val.Add(new_val);
        case AggregationType::MIN:
            return new_val.CompareLessThan(running_val) ? new_val : running_val;
        case AggregationType::MAX:
            return new_val.CompareGreaterThan(running_val) ? new_val : running_val;
        default:
            return running_val;
        }
    }

} // namespace tetodb