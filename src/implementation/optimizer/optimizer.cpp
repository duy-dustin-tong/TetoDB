// optimizer.cpp

#include "optimizer/optimizer.h"
#include <iostream>

#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/topn_plan.h"

#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"

namespace tetodb {

    const AbstractPlanNode* Optimizer::Optimize(const AbstractPlanNode* plan) {
        return OptimizeCustomRules(plan);
    }

    const AbstractPlanNode* Optimizer::OptimizeCustomRules(const AbstractPlanNode* plan) {
        if (!plan) return nullptr;

        PlanType type = plan->GetPlanType();

        // 1. Recurse through Filter
        if (type == PlanType::Filter) {
            const auto* filter_plan = static_cast<const FilterPlanNode*>(plan);
            const AbstractPlanNode* optimized_child = OptimizeCustomRules(filter_plan->GetChildPlan());

            auto new_filter = std::make_unique<FilterPlanNode>(filter_plan->OutputSchema(), optimized_child, filter_plan->GetPredicate());
            const AbstractPlanNode* new_filter_ptr = new_filter.get();
            optimized_nodes_.push_back(std::move(new_filter));

            // Apply SeqScan to IndexScan Rule
            return OptimizeSeqScanAsIndexScan(new_filter_ptr);
        }

        // 2. Recurse through Sort
        if (type == PlanType::Sort) {
            const auto* sort_plan = static_cast<const SortPlanNode*>(plan);
            const AbstractPlanNode* optimized_child = OptimizeCustomRules(sort_plan->GetChildPlan());

            auto new_sort = std::make_unique<SortPlanNode>(sort_plan->OutputSchema(), optimized_child, sort_plan->GetOrderBys());
            const AbstractPlanNode* new_sort_ptr = new_sort.get();
            optimized_nodes_.push_back(std::move(new_sort));
            return new_sort_ptr;
        }

        // 3. Recurse through Limit
        if (type == PlanType::Limit) {
            const auto* limit_plan = static_cast<const LimitPlanNode*>(plan);
            const AbstractPlanNode* optimized_child = OptimizeCustomRules(limit_plan->GetChildPlan());

            auto new_limit = std::make_unique<LimitPlanNode>(limit_plan->OutputSchema(), optimized_child, limit_plan->GetLimit(), limit_plan->GetOffset());
            const AbstractPlanNode* new_limit_ptr = new_limit.get();
            optimized_nodes_.push_back(std::move(new_limit));

            // Apply Sort + Limit to TopN Rule
            return OptimizeSortLimitAsTopN(new_limit_ptr);
        }

        // 4. Recurse through Projection
        if (type == PlanType::Projection) {
            const auto* proj_plan = static_cast<const ProjectionPlanNode*>(plan);
            const AbstractPlanNode* optimized_child = OptimizeCustomRules(proj_plan->GetChildPlan());

            auto new_proj = std::make_unique<ProjectionPlanNode>(proj_plan->OutputSchema(), optimized_child, proj_plan->GetExpressions());
            const AbstractPlanNode* new_proj_ptr = new_proj.get();
            optimized_nodes_.push_back(std::move(new_proj));
            return new_proj_ptr;
        }

        // 5. Recurse through NestedLoopJoin
        if (type == PlanType::NestedLoopJoin) {
            const auto* nlj_plan = static_cast<const NestedLoopJoinPlanNode*>(plan);
            const AbstractPlanNode* opt_left = OptimizeCustomRules(nlj_plan->GetLeftPlan());
            const AbstractPlanNode* opt_right = OptimizeCustomRules(nlj_plan->GetRightPlan());

            auto opt_nlj = std::make_unique<NestedLoopJoinPlanNode>(nlj_plan->OutputSchema(), opt_left, opt_right, nlj_plan->Predicate());
            const AbstractPlanNode* opt_nlj_ptr = opt_nlj.get();
            optimized_nodes_.push_back(std::move(opt_nlj));

            // Apply NLJ to HashJoin Rule
            return OptimizeNLJToHashJoin(opt_nlj_ptr);
        }

        return plan;
    }

    const AbstractPlanNode* Optimizer::OptimizeNLJToHashJoin(const AbstractPlanNode* plan) {
        if (plan->GetPlanType() != PlanType::NestedLoopJoin) return plan;
        const auto* nlj_plan = static_cast<const NestedLoopJoinPlanNode*>(plan);

        const AbstractExpression* predicate = nlj_plan->Predicate();
        if (!predicate) return plan;

        const auto* comp_expr = dynamic_cast<const ComparisonExpression*>(predicate);
        if (!comp_expr) return plan;

        if (comp_expr->GetCompType() == CompType::EQUAL) {


            auto hash_join = std::make_unique<HashJoinPlanNode>(
                nlj_plan->OutputSchema(),
                nlj_plan->GetLeftPlan(),
                nlj_plan->GetRightPlan(),
                comp_expr->GetChildAt(0),
                comp_expr->GetChildAt(1)
            );

            const AbstractPlanNode* hj_ptr = hash_join.get();
            optimized_nodes_.push_back(std::move(hash_join));
            return hj_ptr;
        }

        return plan;
    }

    const AbstractPlanNode* Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNode* plan) {
        if (plan->GetPlanType() != PlanType::Limit) return plan;
        const auto* limit_plan = static_cast<const LimitPlanNode*>(plan);
        const AbstractPlanNode* child = limit_plan->GetChildPlan();

        // Pattern 1: Direct Limit -> Sort
        if (child->GetPlanType() == PlanType::Sort) {
            const auto* sort_plan = static_cast<const SortPlanNode*>(child);


            auto topn_plan = std::make_unique<TopNPlanNode>(
                limit_plan->OutputSchema(), sort_plan->GetChildPlan(),
                sort_plan->GetOrderBys(), limit_plan->GetLimit(), limit_plan->GetOffset()
            );

            const AbstractPlanNode* topn_ptr = topn_plan.get();
            optimized_nodes_.push_back(std::move(topn_plan));
            return topn_ptr;
        }

        // Pattern 2: Limit -> Projection -> Sort (Standard SELECT *)
        if (child->GetPlanType() == PlanType::Projection) {
            const auto* proj_plan = static_cast<const ProjectionPlanNode*>(child);
            if (proj_plan->GetChildPlan()->GetPlanType() == PlanType::Sort) {
                const auto* sort_plan = static_cast<const SortPlanNode*>(proj_plan->GetChildPlan());



                // 1. Create TopN to replace the Sort
                auto topn_plan = std::make_unique<TopNPlanNode>(
                    sort_plan->OutputSchema(), sort_plan->GetChildPlan(),
                    sort_plan->GetOrderBys(), limit_plan->GetLimit(), limit_plan->GetOffset()
                );

                // 2. Re-wrap the Projection on top of the TopN
                auto new_proj = std::make_unique<ProjectionPlanNode>(
                    proj_plan->OutputSchema(), topn_plan.get(), proj_plan->GetExpressions()
                );

                optimized_nodes_.push_back(std::move(topn_plan));
                const AbstractPlanNode* result = new_proj.get();
                optimized_nodes_.push_back(std::move(new_proj));
                return result;
            }
        }

        return plan;
    }

    const AbstractPlanNode* Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNode* plan) {
        if (plan->GetPlanType() != PlanType::Filter) return plan;
        const auto* filter_plan = static_cast<const FilterPlanNode*>(plan);

        if (filter_plan->GetChildPlan()->GetPlanType() != PlanType::SeqScan) return plan;
        const auto* seq_scan = static_cast<const SeqScanPlanNode*>(filter_plan->GetChildPlan());

        const auto* comp_expr = dynamic_cast<const ComparisonExpression*>(filter_plan->GetPredicate());
        if (!comp_expr || comp_expr->GetCompType() != CompType::EQUAL) return plan;

        const auto* col_expr = dynamic_cast<const ColumnValueExpression*>(comp_expr->GetChildAt(0));
        const auto* const_expr = dynamic_cast<const ConstantValueExpression*>(comp_expr->GetChildAt(1));

        if (!col_expr || !const_expr) return plan;

        table_oid_t table_oid = seq_scan->GetTableOid();
        auto table_indexes = catalog_->GetTableIndexes(table_oid);
        TableMetadata* table_info = catalog_->GetTable(table_oid);

        for (auto* index_info : table_indexes) {
            if (index_info->key_attrs_.size() == 1 && index_info->key_attrs_[0] == col_expr->GetColIdx()) {



                // Extract constant value to query the B+ Tree
                Value search_val = const_expr->Evaluate(nullptr, nullptr);

                // Pack the value into a Tuple using the schema of the indexed column
                std::vector<Column> key_cols;
                key_cols.push_back(table_info->schema_.GetColumn(col_expr->GetColIdx()));
                Schema key_schema(key_cols);
                Tuple key_tuple({ search_val }, &key_schema);

                auto index_scan = std::make_unique<IndexScanPlanNode>(
                    filter_plan->OutputSchema(),
                    index_info->oid_,
                    table_oid,
                    key_tuple
                );

                const AbstractPlanNode* is_ptr = index_scan.get();
                optimized_nodes_.push_back(std::move(index_scan));
                return is_ptr;
            }
        }

        return plan;
    }

} // namespace tetodb