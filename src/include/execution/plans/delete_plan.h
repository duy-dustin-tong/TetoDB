// delete_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "catalog/catalog.h"

namespace tetodb {

    class DeletePlanNode : public AbstractPlanNode {
    public:
        // Deletes don't typically output a schema (they return success/counts)
        DeletePlanNode(const AbstractPlanNode* child, table_oid_t table_oid)
            : AbstractPlanNode(nullptr, PlanType::Delete), child_(child), table_oid_(table_oid) {
        }

        inline const AbstractPlanNode* GetChildPlan() const { return child_; }
        inline table_oid_t GetTableOid() const { return table_oid_; }

        std::string ToString() const override { return "Delete [Table OID: " + std::to_string(table_oid_) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return { child_ }; }

    private:
        const AbstractPlanNode* child_;
        table_oid_t table_oid_;
    };

} // namespace tetodb