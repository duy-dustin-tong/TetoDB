// insert_plan.h

#pragma once

#include <vector>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"
#include "catalog/catalog.h"

namespace tetodb {

    class InsertPlanNode : public AbstractPlanNode {
    public:
        InsertPlanNode(table_oid_t table_oid, std::vector<std::vector<const AbstractExpression*>> raw_exprs)
            : AbstractPlanNode(nullptr, PlanType::Insert), table_oid_(table_oid), raw_exprs_(std::move(raw_exprs)) {
        }

        inline table_oid_t GetTableOid() const { return table_oid_; }
        inline const std::vector<std::vector<const AbstractExpression*>>& GetRawExpressions() const { return raw_exprs_; }

        std::string ToString() const override { return "Insert [Table OID: " + std::to_string(table_oid_) + ", Rows: " + std::to_string(raw_exprs_.size()) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return {}; }

    private:
        table_oid_t table_oid_;
        std::vector<std::vector<const AbstractExpression*>> raw_exprs_;
    };

} // namespace tetodb