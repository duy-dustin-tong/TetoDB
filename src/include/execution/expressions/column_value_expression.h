// column_value_expression.h

#pragma once

#include "execution/expressions/abstract_expression.h"

namespace tetodb {

    class ColumnValueExpression : public AbstractExpression {
    public:
        // ADDED: tuple_idx. 0 = Left Tuple, 1 = Right Tuple. Defaults to 0 for SeqScans.
        ColumnValueExpression(uint32_t tuple_idx, uint32_t col_idx)
            : AbstractExpression({}), tuple_idx_(tuple_idx), col_idx_(col_idx) {
        }

        Value Evaluate(const Tuple* tuple, const Schema* schema, const std::vector<Value>* params = nullptr) const override {
            return tuple->GetValue(schema, col_idx_);
        }

        Value EvaluateJoin(const Tuple* left_tuple, const Schema* left_schema,
            const Tuple* right_tuple, const Schema* right_schema, const std::vector<Value>* params = nullptr) const override {
            if (tuple_idx_ == 0) return left_tuple->GetValue(left_schema, col_idx_);
            else return right_tuple->GetValue(right_schema, col_idx_);
        }

        inline uint32_t GetColIdx() const { return col_idx_; }
        inline uint32_t GetTupleIdx() const { return tuple_idx_; }

    private:
        uint32_t tuple_idx_;
        uint32_t col_idx_;
    };

} // namespace tetodb