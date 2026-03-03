// parameter_value_expression.h

#pragma once

#include "execution/expressions/abstract_expression.h"
#include <stdexcept>

namespace tetodb {

    class ParameterValueExpression : public AbstractExpression {
    public:
        ParameterValueExpression(uint32_t param_idx)
            : AbstractExpression({}), param_idx_(param_idx) {
        }

        Value Evaluate(const Tuple* tuple, const Schema* schema, const std::vector<Value>* params = nullptr) const override {
            if (!params || param_idx_ == 0 || param_idx_ > params->size()) {
                throw std::runtime_error("Execution Error: Invalid parameter index $" + std::to_string(param_idx_));
            }
            // 1-indexed in SQL ($1, $2), so subtract 1 for the vector
            return (*params)[param_idx_ - 1];
        }

        Value EvaluateJoin(const Tuple* left_tuple, const Schema* left_schema,
            const Tuple* right_tuple, const Schema* right_schema, const std::vector<Value>* params = nullptr) const override {
            if (!params || param_idx_ == 0 || param_idx_ > params->size()) {
                throw std::runtime_error("Execution Error: Invalid parameter index $" + std::to_string(param_idx_));
            }
            return (*params)[param_idx_ - 1];
        }

    private:
        uint32_t param_idx_;
    };

} // namespace tetodb