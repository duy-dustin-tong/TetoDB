#pragma once

#include <string>
#include "execution/plans/abstract_plan.h"
#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class SeqScanPlanNode : public AbstractPlanNode {
    public:
        /**
         * @param output_schema The schema of the data we are returning
         * @param table_name The name of the table to scan
         * @param predicate The WHERE clause AST (nullptr if no WHERE clause)
         */

        SeqScanPlanNode(const Schema* output_schema,
            table_oid_t table_oid, // <-- OID
            const AbstractExpression* predicate = nullptr)
            : AbstractPlanNode(output_schema, PlanType::SeqScan),
            table_oid_(table_oid),
            predicate_(predicate) {
        }

        inline table_oid_t GetTableOid() const { return table_oid_; }
        const AbstractExpression* GetPredicate() const { return predicate_; }

        std::string ToString() const override { return "SeqScan [Table OID: " + std::to_string(table_oid_) + "]"; }
        std::vector<const AbstractPlanNode*> GetChildren() const override { return {}; }

    private:
        table_oid_t table_oid_;
        const AbstractExpression* predicate_;

        
    };

} // namespace tetodb