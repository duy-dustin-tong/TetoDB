// constant_value_expression.h

#pragma once

#include "execution/expressions/abstract_expression.h"

namespace tetodb {

class ConstantValueExpression : public AbstractExpression {
public:
  ConstantValueExpression(const Value &val)
      : AbstractExpression({}), val_(val) {}

  Value Evaluate(const Tuple *tuple, const Schema *schema,
                 const std::vector<Value> *params = nullptr) const override {
    return val_;
  }

  Value
  EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
               const Tuple *right_tuple, const Schema *right_schema,
               const std::vector<Value> *params = nullptr) const override {
    return val_;
  }

  TypeId GetReturnType() const override { return val_.GetTypeId(); }

private:
  Value val_;
};

} // namespace tetodb