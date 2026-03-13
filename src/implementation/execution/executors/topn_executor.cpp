// topn_executor.cpp

#include "execution/executors/topn_executor.h"

namespace tetodb {

    void TopNExecutor::Init() {
        child_executor_->Init();
        top_entries_.clear();
        cursor_ = 0;

        // Lambda comparator for the priority queue
        auto comp = [this](const Tuple& a, const Tuple& b) {
            const Schema* schema = plan_->GetChildPlan()->OutputSchema();

            for (const auto& order_by : plan_->GetOrderBys()) {
                OrderByType type = order_by.first;
                Value val_a = order_by.second->Evaluate(&a, schema);
                Value val_b = order_by.second->Evaluate(&b, schema);

                if (val_a.CompareEquals(val_b)) continue;

                // Priority Queue maintains the "worst" of the Top-N at the top so it can be popped easily.
                if (type == OrderByType::DESC) {
                    return val_a.CompareGreaterThan(val_b); // Min-Heap for DESC
                }
                else {
                    return val_a.CompareLessThan(val_b);    // Max-Heap for ASC
                }
            }
            return false;
            };

        std::priority_queue<Tuple, std::vector<Tuple>, decltype(comp)> pq(comp);

        Tuple tuple;
        RID rid;
        uint32_t target_size = plan_->GetLimit() + plan_->GetOffset();

        // Stream tuples through the heap. O(N log K) time, O(K) memory.
        while (child_executor_->Next(&tuple, &rid)) {
            pq.push(tuple);
            if (pq.size() > target_size) {
                pq.pop();
            }
        }

        // Drain the queue (Worst to Best)
        while (!pq.empty()) {
            top_entries_.push_back(pq.top());
            pq.pop();
        }

        // Reverse to correct order (Best to Worst)
        std::reverse(top_entries_.begin(), top_entries_.end());

        // Apply Offset
        if (plan_->GetOffset() > 0 && plan_->GetOffset() < top_entries_.size()) {
            top_entries_.erase(top_entries_.begin(), top_entries_.begin() + plan_->GetOffset());
        }
        else if (plan_->GetOffset() >= top_entries_.size()) {
            top_entries_.clear();
        }
    }

    const Schema* TopNExecutor::GetOutputSchema() {
        return plan_->OutputSchema(); 
    }

    bool TopNExecutor::Next(Tuple* tuple, RID* rid) {
        if (cursor_ >= top_entries_.size()) return false;

        *tuple = top_entries_[cursor_];
        *rid = tuple->GetRid();
        cursor_++;
        return true;
    }

} // namespace tetodb