// comparison_expression.h

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "execution/expressions/abstract_expression.h"
#include "type/value.h"

namespace tetodb {

enum class CompType {
  EQUAL,
  NOT_EQUAL,
  LESS_THAN,
  GREATER_THAN,
  LESS_THAN_OR_EQUAL,
  GREATER_THAN_OR_EQUAL,
  IS_NULL,
  IS_NOT_NULL,
  LIKE,
  ILIKE
};

class ComparisonExpression : public AbstractExpression {
private:
  // Helper to safely move unique_ptrs into the base class vector
  static std::vector<std::unique_ptr<AbstractExpression>>
  MakeChildren(std::unique_ptr<AbstractExpression> left,
               std::unique_ptr<AbstractExpression> right) {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.push_back(std::move(left));
    children.push_back(std::move(right));
    return children;
  }

public:
  ComparisonExpression(CompType comp_type,
                       std::unique_ptr<AbstractExpression> left,
                       std::unique_ptr<AbstractExpression> right)
      : AbstractExpression(MakeChildren(std::move(left), std::move(right))),
        comp_type_(comp_type) {}

  inline CompType GetCompType() const { return comp_type_; }

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

  TypeId GetReturnType() const override { return TypeId::BOOLEAN; }

private:
  Value PerformComputation(const Value &lhs, const Value &rhs) const {
    bool result = false;
    switch (comp_type_) {
    case CompType::EQUAL:
      result = lhs.CompareEquals(rhs);
      break;
    case CompType::NOT_EQUAL:
      result = lhs.CompareNotEquals(rhs);
      break;
    case CompType::LESS_THAN:
      result = lhs.CompareLessThan(rhs);
      break;
    case CompType::GREATER_THAN:
      result = lhs.CompareGreaterThan(rhs);
      break;
      // Safe Fallbacks using logical inversion!
    case CompType::LESS_THAN_OR_EQUAL:
      result = !lhs.CompareGreaterThan(rhs);
      break;
    case CompType::GREATER_THAN_OR_EQUAL:
      result = !lhs.CompareLessThan(rhs);
      break;
    case CompType::IS_NULL:
      result = lhs.IsNull();
      break;
    case CompType::IS_NOT_NULL:
      result = !lhs.IsNull();
      break;
    case CompType::LIKE:
      result = lhs.CompareLike(rhs);
      break;
    case CompType::ILIKE:
      result = lhs.CompareILike(rhs);
      break;
    default:
      throw std::runtime_error("Unknown Comparison Type!");
    }
    return Value(TypeId::BOOLEAN, result);
  }

  CompType comp_type_;
};

} // namespace tetodb