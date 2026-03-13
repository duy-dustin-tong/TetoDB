// string_expression.h
#pragma once

#include <algorithm>
#include <cctype>
#include <memory>
#include <string>
#include <vector>

#include "execution/expressions/abstract_expression.h"
#include "type/value.h"

namespace tetodb {

enum class StringFuncType { UPPER, LOWER, LENGTH, CONCAT, SUBSTRING };

class StringExpression : public AbstractExpression {
public:
  StringExpression(StringFuncType func_type,
                   std::vector<std::unique_ptr<AbstractExpression>> children)
      : AbstractExpression(std::move(children)), func_type_(func_type) {}

  Value Evaluate(const Tuple *tuple, const Schema *schema,
                 const std::vector<Value> *params = nullptr) const override {

    std::vector<Value> args;
    for (const auto &child : GetChildren()) {
      Value val = child->Evaluate(tuple, schema, params);
      if (val.IsNull()) {
        // Standard SQL behavior: If any argument is NULL, the result is NULL.
        if (func_type_ == StringFuncType::LENGTH) {
          return Value::GetNullValue(TypeId::INTEGER);
        }
        return Value::GetNullValue(TypeId::VARCHAR);
      }
      args.push_back(val);
    }

    return PerformComputation(args);
  }

  Value
  EvaluateJoin(const Tuple *left_tuple, const Schema *left_schema,
               const Tuple *right_tuple, const Schema *right_schema,
               const std::vector<Value> *params = nullptr) const override {

    std::vector<Value> args;
    for (const auto &child : GetChildren()) {
      Value val = child->EvaluateJoin(left_tuple, left_schema, right_tuple,
                                      right_schema, params);
      if (val.IsNull()) {
        if (func_type_ == StringFuncType::LENGTH) {
          return Value::GetNullValue(TypeId::INTEGER);
        }
        return Value::GetNullValue(TypeId::VARCHAR);
      }
      args.push_back(val);
    }

    return PerformComputation(args);
  }

  TypeId GetReturnType() const override {
    if (func_type_ == StringFuncType::LENGTH) {
      return TypeId::INTEGER;
    }
    return TypeId::VARCHAR;
  }

private:
  Value PerformComputation(const std::vector<Value> &args) const {
    switch (func_type_) {
    case StringFuncType::UPPER: {
      if (args.size() != 1)
        throw std::runtime_error("UPPER expects exactly 1 argument.");
      std::string s = args[0].GetAsString();
      std::transform(s.begin(), s.end(), s.begin(), ::toupper);
      return Value(TypeId::VARCHAR, s);
    }
    case StringFuncType::LOWER: {
      if (args.size() != 1)
        throw std::runtime_error("LOWER expects exactly 1 argument.");
      std::string s = args[0].GetAsString();
      std::transform(s.begin(), s.end(), s.begin(), ::tolower);
      return Value(TypeId::VARCHAR, s);
    }
    case StringFuncType::LENGTH: {
      if (args.size() != 1)
        throw std::runtime_error("LENGTH expects exactly 1 argument.");
      return Value(TypeId::INTEGER,
                   static_cast<int32_t>(args[0].GetAsString().length()));
    }
    case StringFuncType::CONCAT: {
      if (args.size() < 2)
        throw std::runtime_error("CONCAT expects at least 2 arguments.");
      std::string result = "";
      for (const auto &arg : args) {
        result += arg.GetAsString();
      }
      return Value(TypeId::VARCHAR, result);
    }
    case StringFuncType::SUBSTRING: {
      if (args.size() != 3)
        throw std::runtime_error(
            "SUBSTRING expects exactly 3 arguments (string, start, length).");
      std::string s = args[0].GetAsString();
      int32_t start = args[1].CastAsInteger();
      int32_t len = args[2].CastAsInteger();

      // SQL is 1-indexed. C++ is 0-indexed.
      if (start < 1)
        start = 1;
      int32_t cpp_idx = start - 1;

      if (cpp_idx >= static_cast<int32_t>(s.length())) {
        return Value(TypeId::VARCHAR, std::string(""));
      }

      if (len < 0)
        len = 0;
      return Value(TypeId::VARCHAR, s.substr(cpp_idx, len));
    }
    default:
      throw std::runtime_error("Unknown string function type.");
    }
  }

  StringFuncType func_type_;
};

} // namespace tetodb
