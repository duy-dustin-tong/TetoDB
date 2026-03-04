// in_expression.h
#pragma once

#include <memory>
#include <vector>

#include "execution/expressions/abstract_expression.h"
#include "type/value.h"

namespace tetodb {

class InExpression : public AbstractExpression {
private:
  static std::vector<std::unique_ptr<AbstractExpression>>
  MakeChildren(std::unique_ptr<AbstractExpression> left,
               std::vector<std::unique_ptr<AbstractExpression>> list) {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    children.push_back(std::move(left));
    for (auto &item : list) {
      children.push_back(std::move(item));
    }
    return children;
  }

public:
  InExpression(std::unique_ptr<AbstractExpression> left,
               std::vector<std::unique_ptr<AbstractExpression>> list,
               bool is_not = false)
      : AbstractExpression(MakeChildren(std::move(left), std::move(list))),
        is_not_(is_not) {}

  Value Evaluate(const Tuple *tuple, const Schema *schema,
                 const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema, params);

    if (lhs.IsNull()) {
      return Value::GetNullValue(TypeId::BOOLEAN);
    }

    bool has_null_in_list = false;

    for (size_t i = 1; i < GetChildren().size(); ++i) {
      Value rhs = GetChildAt(i)->Evaluate(tuple, schema, params);
      if (rhs.IsNull()) {
        has_null_in_list = true;
        continue;
      }
      if (lhs.CompareEquals(rhs)) {
        return Value(TypeId::BOOLEAN, is_not_ ? false : true);
      }
    }

    if (has_null_in_list) {
      return Value::GetNullValue(TypeId::BOOLEAN);
    }

    return Value(TypeId::BOOLEAN, is_not_ ? true : false);
  }

  Value
  EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
               const Tuple *right_tuple, const Schema *right_schema,
               const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema,
                                            right_tuple, right_schema, params);

    if (lhs.IsNull()) {
      return Value::GetNullValue(TypeId::BOOLEAN);
    }

    bool has_null_in_list = false;

    for (size_t i = 1; i < GetChildren().size(); ++i) {
      Value rhs = GetChildAt(i)->EvaluateJoin(
          left_tuple, left_schema, right_tuple, right_schema, params);
      if (rhs.IsNull()) {
        has_null_in_list = true;
        continue;
      }
      if (lhs.CompareEquals(rhs)) {
        return Value(TypeId::BOOLEAN, is_not_ ? false : true);
      }
    }

    if (has_null_in_list) {
      return Value::GetNullValue(TypeId::BOOLEAN);
    }

    return Value(TypeId::BOOLEAN, is_not_ ? true : false);
  }

private:
  bool is_not_;
};

} // namespace tetodb
