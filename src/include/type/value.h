// value.h

#pragma once

#include <algorithm>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>


#include "type/type_id.h"

namespace tetodb {

class Value {
public:
  static Value GetNullValue(TypeId type) {
    Value v;
    v.type_id_ = type;
    v.is_null_ = true;
    return v;
  }

  // --- Constructors ---

  // 1. Invalid / Null
  Value() : type_id_(TypeId::INVALID), is_null_(true) {
    memset(&val_, 0, sizeof(Val));
  }

  // 2. Integers (Tiny, Small, Int, Big)
  Value(TypeId type, int8_t i) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.tinyint_ = i;
  }

  Value(TypeId type, int16_t i) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.smallint_ = i;
  }

  Value(TypeId type, int32_t i) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.integer_ = i;
  }

  Value(TypeId type, int64_t i) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.bigint_ = i;
  }

  // 3. Decimals
  Value(TypeId type, double d) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.decimal_ = d;
  }

  // 4. Booleans
  Value(TypeId type, bool b) : type_id_(type), is_null_(false) {
    memset(&val_, 0, sizeof(Val));
    val_.boolean_ = b;
  }

  // 5. Strings (Varchar)
  Value(TypeId type, std::string s)
      : type_id_(type), is_null_(false), str_value_(std::move(s)) {}

  Value(TypeId type, const char *s)
      : type_id_(type), is_null_(false), str_value_(s) {}

  // --- Copy / Move Semantics ---

  // CRITICAL FIX: is_null_ added to the initialization list
  Value(const Value &other)
      : type_id_(other.type_id_), is_null_(other.is_null_),
        str_value_(other.str_value_) {
    val_ = other.val_;
  }

  Value &operator=(const Value &other) {
    if (this != &other) {
      type_id_ = other.type_id_;
      val_ = other.val_;
      str_value_ = other.str_value_;
      is_null_ = other.is_null_; // Preserves Null State
    }
    return *this;
  }

  // --- Accessors ---
  inline TypeId GetTypeId() const { return type_id_; }

  // Helper
  inline static bool IsNumeric(TypeId type) {
    return type == TypeId::TINYINT || type == TypeId::SMALLINT ||
           type == TypeId::INTEGER || type == TypeId::BIGINT ||
           type == TypeId::DECIMAL;
  }

  // Fast accessors (Use when type is known)
  inline int8_t GetAsTinyInt() const { return val_.tinyint_; }
  inline int16_t GetAsSmallInt() const { return val_.smallint_; }
  inline int32_t GetAsInteger() const { return val_.integer_; }
  inline int64_t GetAsBigInt() const { return val_.bigint_; }
  inline double GetAsDecimal() const { return val_.decimal_; }
  inline bool GetAsBoolean() const { return val_.boolean_; }
  inline std::string GetAsString() const { return str_value_; }

  inline bool IsNull() const { return is_null_; }

  // --- Comparison Operations ---
  bool CompareEquals(const Value &other) const;
  bool CompareNotEquals(const Value &other) const;
  bool CompareLessThan(const Value &other) const;
  bool CompareGreaterThan(const Value &other) const;
  bool CompareLike(const Value &other) const;
  bool CompareILike(const Value &other) const;

  // --- Numeric Promotion & Casting ---
  double CastAsDouble() const;
  int64_t CastAsBigInt() const;
  int32_t CastAsInteger() const;

  // --- Arithmetic Operations ---
  Value Add(const Value &other) const;
  Value Subtract(const Value &other) const;
  Value Multiply(const Value &other) const;
  Value Divide(const Value &other) const;

  // --- Serialization ---
  uint32_t SerializeTo(char *storage) const;
  static Value DeserializeFrom(const char *storage, TypeId type);
  uint32_t GetSize() const;

  std::size_t Hash() const;

  // --- Debugging ---
  std::string ToString() const;

private:
  TypeId type_id_;
  bool is_null_{false};
  union Val {
    int8_t tinyint_;
    int16_t smallint_;
    int32_t integer_;
    int64_t bigint_;
    double decimal_;
    bool boolean_;
  } val_;

  std::string str_value_;
};

} // namespace tetodb