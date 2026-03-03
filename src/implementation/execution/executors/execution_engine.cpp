// execution_engine.cpp

#include "execution/execution_engine.h"

// --- Plan Nodes ---
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/insert_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/update_plan.h"
#include "execution/plans/delete_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/topn_plan.h"

// --- Executors ---
#include "execution/executors/seq_scan_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/projection_executor.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/executors/nested_loop_join_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/executors/update_executor.h"
#include "execution/executors/delete_executor.h"
#include "execution/executors/index_scan_executor.h"
#include "execution/executors/filter_executor.h"
#include "execution/executors/sort_executor.h"
#include "execution/executors/limit_executor.h"
#include "execution/executors/topn_executor.h" 

namespace tetodb {

    std::unique_ptr<AbstractExecutor> ExecutionEngine::CreateExecutor(const AbstractPlanNode* plan, ExecutionContext* exec_ctx) {
        if (!plan) return nullptr;

        switch (plan->GetPlanType()) {
        case PlanType::SeqScan: {
            const auto* seq_plan = static_cast<const SeqScanPlanNode*>(plan);
            return std::make_unique<SeqScanExecutor>(exec_ctx, seq_plan);
        }
        case PlanType::Insert: {
            const auto* insert_plan = static_cast<const InsertPlanNode*>(plan);
            return std::make_unique<InsertExecutor>(exec_ctx, insert_plan);
        }
        case PlanType::Projection: {
            const auto* proj_plan = static_cast<const ProjectionPlanNode*>(plan);
            auto child = CreateExecutor(proj_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<ProjectionExecutor>(exec_ctx, proj_plan, std::move(child));
        }
        case PlanType::HashJoin: {
            const auto* hj_plan = static_cast<const HashJoinPlanNode*>(plan);
            auto left = CreateExecutor(hj_plan->GetLeftPlan(), exec_ctx);
            auto right = CreateExecutor(hj_plan->GetRightPlan(), exec_ctx);
            return std::make_unique<HashJoinExecutor>(exec_ctx, hj_plan, std::move(left), std::move(right));
        }
        case PlanType::NestedLoopJoin: {
            const auto* nlj_plan = static_cast<const NestedLoopJoinPlanNode*>(plan);
            auto left = CreateExecutor(nlj_plan->GetLeftPlan(), exec_ctx);
            auto right = CreateExecutor(nlj_plan->GetRightPlan(), exec_ctx);
            return std::make_unique<NestedLoopJoinExecutor>(exec_ctx, nlj_plan, std::move(left), std::move(right));
        }
        case PlanType::Aggregation: {
            const auto* agg_plan = static_cast<const AggregationPlanNode*>(plan);
            auto child = CreateExecutor(agg_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<AggregationExecutor>(exec_ctx, agg_plan, std::move(child));
        }
        case PlanType::Update: {
            const auto* update_plan = static_cast<const UpdatePlanNode*>(plan);
            auto child = CreateExecutor(update_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child));
        }
        case PlanType::Delete: {
            const auto* delete_plan = static_cast<const DeletePlanNode*>(plan);
            auto child = CreateExecutor(delete_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child));
        }
        case PlanType::IndexScan: {
            const auto* index_plan = static_cast<const IndexScanPlanNode*>(plan);
            return std::make_unique<IndexScanExecutor>(exec_ctx, index_plan);
        }
        case PlanType::Filter: {
            const auto* filter_plan = static_cast<const FilterPlanNode*>(plan);
            auto child = CreateExecutor(filter_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<FilterExecutor>(exec_ctx, filter_plan, std::move(child));
        }
        case PlanType::Sort: {
            const auto* sort_plan = static_cast<const SortPlanNode*>(plan);
            auto child = CreateExecutor(sort_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<SortExecutor>(exec_ctx, sort_plan, std::move(child));
        }
        case PlanType::Limit: {
            const auto* limit_plan = static_cast<const LimitPlanNode*>(plan);
            auto child = CreateExecutor(limit_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<LimitExecutor>(exec_ctx, limit_plan, std::move(child));
        }
        case PlanType::TopN: { 
            const auto* topn_plan = static_cast<const TopNPlanNode*>(plan);
            auto child = CreateExecutor(topn_plan->GetChildPlan(), exec_ctx);
            return std::make_unique<TopNExecutor>(exec_ctx, topn_plan, std::move(child));
        }
        default:
            throw std::runtime_error("ExecutionEngine Error: Unsupported PlanType");
        }
    }

} // namespace tetodb