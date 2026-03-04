// logic_expression.h
#pragma once

#include <memory>
#include <string>
#include <vector>


#include "execution/expressions/abstract_expression.h"
#include "type/value.h"

namespace tetodb {

enum class LogicType { AND, OR, NOT };

class LogicExpression : public AbstractExpression {
private:
  static std::vector<std::unique_ptr<AbstractExpression>>
  MakeChildren(std::unique_ptr<AbstractExpression> left,
               std::unique_ptr<AbstractExpression> right) {
    std::vector<std::unique_ptr<AbstractExpression>> children;
    if (left)
      children.push_back(std::move(left));
    if (right)
      children.push_back(std::move(right));
    return children;
  }

public:
  LogicExpression(LogicType logic_type,
                  std::unique_ptr<AbstractExpression> left,
                  std::unique_ptr<AbstractExpression> right = nullptr)
      : AbstractExpression(MakeChildren(std::move(left), std::move(right))),
        logic_type_(logic_type) {}

  Value Evaluate(const Tuple *tuple, const Schema *schema,
                 const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->Evaluate(tuple, schema, params);

    if (logic_type_ == LogicType::NOT) {
      if (lhs.IsNull())
        return Value::GetNullValue(TypeId::BOOLEAN);
      return Value(TypeId::BOOLEAN, !lhs.GetAsBoolean());
    }

    Value rhs = GetChildAt(1)->Evaluate(tuple, schema, params);

    // SQL NULL semantics for AND/OR
    if (lhs.IsNull() || rhs.IsNull()) {
      if (logic_type_ == LogicType::AND) {
        if (!lhs.IsNull() && !lhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, false);
        if (!rhs.IsNull() && !rhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, false);
        return Value::GetNullValue(TypeId::BOOLEAN);
      } else { // OR
        if (!lhs.IsNull() && lhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, true);
        if (!rhs.IsNull() && rhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, true);
        return Value::GetNullValue(TypeId::BOOLEAN);
      }
    }

    if (logic_type_ == LogicType::AND) {
      return Value(TypeId::BOOLEAN, lhs.GetAsBoolean() && rhs.GetAsBoolean());
    } else { // OR
      return Value(TypeId::BOOLEAN, lhs.GetAsBoolean() || rhs.GetAsBoolean());
    }
  }

  Value
  EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
               const Tuple *right_tuple, const Schema *right_schema,
               const std::vector<Value> *params = nullptr) const override {
    Value lhs = GetChildAt(0)->EvaluateJoin(left_tuple, left_schema,
                                            right_tuple, right_schema, params);

    if (logic_type_ == LogicType::NOT) {
      if (lhs.IsNull())
        return Value::GetNullValue(TypeId::BOOLEAN);
      return Value(TypeId::BOOLEAN, !lhs.GetAsBoolean());
    }

    Value rhs = GetChildAt(1)->EvaluateJoin(left_tuple, left_schema,
                                            right_tuple, right_schema, params);

    // SQL NULL semantics for AND/OR
    if (lhs.IsNull() || rhs.IsNull()) {
      if (logic_type_ == LogicType::AND) {
        if (!lhs.IsNull() && !lhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, false);
        if (!rhs.IsNull() && !rhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, false);
        return Value::GetNullValue(TypeId::BOOLEAN);
      } else { // OR
        if (!lhs.IsNull() && lhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, true);
        if (!rhs.IsNull() && rhs.GetAsBoolean())
          return Value(TypeId::BOOLEAN, true);
        return Value::GetNullValue(TypeId::BOOLEAN);
      }
    }

    if (logic_type_ == LogicType::AND) {
      return Value(TypeId::BOOLEAN, lhs.GetAsBoolean() && rhs.GetAsBoolean());
    } else { // OR
      return Value(TypeId::BOOLEAN, lhs.GetAsBoolean() || rhs.GetAsBoolean());
    }
  }

private:
  LogicType logic_type_;
};

} // namespace tetodb
