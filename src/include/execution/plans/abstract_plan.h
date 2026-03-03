// abstract_plan.h

#pragma once

#include "catalog/schema.h"

namespace tetodb {

    /**
     * PlanType represents the types of plans that we can have in our system.
     */
    enum class PlanType {
        // --- Data Access ---
        SeqScan,
        IndexScan,

        // --- Mutations ---
        Insert,
        Update,
        Delete,

        // --- Joins ---
        NestedLoopJoin,
        HashJoin,

        // --- Structure & Math ---
        Aggregation,
        Projection,

        // --- The Final Three (Gatekeepers & Formatting) ---
        Filter,     // Handles the WHERE / HAVING clauses
        Sort,       // Handles the ORDER BY clause
        Limit,       // Handles the LIMIT / OFFSET clauses

        TopN
    };

    class AbstractPlanNode {
    public:
        virtual ~AbstractPlanNode() = default;

        inline const Schema* OutputSchema() const { return output_schema_; }
        inline PlanType GetPlanType() const { return plan_type_; }

        virtual std::string ToString() const = 0;
        virtual std::vector<const AbstractPlanNode*> GetChildren() const = 0;

    protected:
        AbstractPlanNode(const Schema* output_schema, PlanType plan_type)
            : output_schema_(output_schema), plan_type_(plan_type) {
        }

        const Schema* output_schema_;
        PlanType plan_type_;
    };
}  // namespace tetodb