// index_scan_plan.h

#pragma once

#include "execution/plans/abstract_plan.h"
#include "storage/table/tuple.h"
#include <string>

namespace tetodb {

    enum class IndexType { BTREE, HASH };

    class IndexScanPlanNode : public AbstractPlanNode {
    public:
        IndexScanPlanNode(const Schema* output_schema,
            index_oid_t index_oid,
            table_oid_t table_oid,
            Tuple key_tuple,
            IndexType index_type = IndexType::BTREE) // Defaults to BTREE
            : AbstractPlanNode(output_schema, PlanType::IndexScan),
            index_oid_(index_oid),
            table_oid_(table_oid),
            key_tuple_(std::move(key_tuple)),
            index_type_(index_type) {
        }

        inline index_oid_t GetIndexOid() const { return index_oid_; }
        inline table_oid_t GetTableOid() const { return table_oid_; }
        inline Tuple GetSearchKey() const { return key_tuple_; }
        inline IndexType GetIndexType() const { return index_type_; }

        std::string ToString() const override {
            std::string type_str = (index_type_ == IndexType::BTREE) ? "B+Tree" : "Hash";
            return "IndexScan [Index OID: " + std::to_string(index_oid_) +
                ", Table OID: " + std::to_string(table_oid_) +
                ", Type: " + type_str + "]";
        }

        std::vector<const AbstractPlanNode*> GetChildren() const override { return {}; }

    private:
        index_oid_t index_oid_;
        table_oid_t table_oid_;
        Tuple key_tuple_;
        IndexType index_type_;
    };
}  // namespace tetodb