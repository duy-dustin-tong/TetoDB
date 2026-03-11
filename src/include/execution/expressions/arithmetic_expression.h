// arithmetic_expression.h

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>


#include "execution/expressions/abstract_expression.h"
#include "type/value.h"

namespace tetodb {

enum class ArithType { ADD, SUBTRACT, MULTIPLY, DIVIDE };

class ArithmeticExpression : public AbstractExpression {
private:
  static std::vector<std::unique_ptr<AbstractExpression>>
  MakeChildren(std::unique_ptr<AbstractExpression> left,
               std::unique_ptr<AbstractExpression> right) {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return children;
  }

public:
  ArithmeticExpression(ArithType arith_type,
                       std::unique_ptr<AbstractExpression> left,
                       std::unique_ptr<AbstractExpression> right)
      : AbstractExpression(MakeChildren(std::move(left), std::move(right))),
        arith_type_(arith_type) {}

  Value Evaluate(const Tuple *tuple, const Schema *schema,
                 const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema, params);
    Value rhs = GetChildAt(1)->Evaluate(tuple, schema, params);
    return PerformComputation(lhs, rhs);
  }

  Value
  EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
               const Tuple *right_tuple, const Schema *right_schema,
               const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema,
                                            right_tuple, right_schema, params);
    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema,
                                            right_tuple, right_schema, params);
    return PerformComputation(lhs, rhs);
  }

  TypeId GetReturnType() const override { 
    TypeId lhs_type = GetChildAt(0)->GetReturnType();
    TypeId rhs_type = GetChildAt(1)->GetReturnType();

    if (lhs_type == TypeId::DECIMAL || rhs_type == TypeId::DECIMAL) {
      return TypeId::DECIMAL;
    }
    if (lhs_type == TypeId::BIGINT || rhs_type == TypeId::BIGINT) {
      return TypeId::BIGINT;
    }
    return TypeId::INTEGER;
  }

private:
  Value PerformComputation(const Value &lhs, const Value &rhs) const {
    switch (arith_type_) {
    case ArithType::ADD:
      return lhs.Add(rhs);
    case ArithType::SUBTRACT:
      return lhs.Subtract(rhs);
    case ArithType::MULTIPLY:
      return lhs.Multiply(rhs);
    case ArithType::DIVIDE:
      return lhs.Divide(rhs);
    default:
      throw std::runtime_error("Unknown Arithmetic Type!");
    }
  }

  ArithType arith_type_;
};

} // namespace tetodb