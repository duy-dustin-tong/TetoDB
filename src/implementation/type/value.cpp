// value.cpp

#include "type/value.h"
#include <stdexcept>
#include <ctime>
#include <iomanip>
#include <sstream>

namespace tetodb {

// ==========================================================
// NUMERIC PROMOTION & CASTING
// ==========================================================

double Value::CastAsDouble() const {
  switch (type_id_) {
  case TypeId::TINYINT:
    return static_cast<double>(val_.tinyint_);
  case TypeId::SMALLINT:
    return static_cast<double>(val_.smallint_);
  case TypeId::INTEGER:
    return static_cast<double>(val_.integer_);
  case TypeId::BIGINT:
    return static_cast<double>(val_.bigint_);
  case TypeId::DECIMAL:
    return val_.decimal_;
  case TypeId::TIMESTAMP:
    return static_cast<double>(val_.bigint_);
  default:
    throw std::runtime_error("Type error: Cannot cast non-numeric to double.");
  }
}

int64_t Value::CastAsBigInt() const {
  switch (type_id_) {
  case TypeId::TINYINT:
    return static_cast<int64_t>(val_.tinyint_);
  case TypeId::SMALLINT:
    return static_cast<int64_t>(val_.smallint_);
  case TypeId::INTEGER:
    return static_cast<int64_t>(val_.integer_);
  case TypeId::BIGINT:
    return val_.bigint_;
  case TypeId::DECIMAL:
    return static_cast<int64_t>(val_.decimal_);
  case TypeId::TIMESTAMP:
    return val_.bigint_;
  default:
    throw std::runtime_error("Type error: Cannot cast non-numeric to BigInt.");
  }
}

int32_t Value::CastAsInteger() const {
  switch (type_id_) {
  case TypeId::TINYINT:
    return static_cast<int32_t>(val_.tinyint_);
  case TypeId::SMALLINT:
    return static_cast<int32_t>(val_.smallint_);
  case TypeId::INTEGER:
    return val_.integer_;
  case TypeId::BIGINT:
    return static_cast<int32_t>(val_.bigint_);
  case TypeId::DECIMAL:
    return static_cast<int32_t>(val_.decimal_);
  default:
    throw std::runtime_error("Type error: Cannot cast non-numeric to Integer.");
  }
}

// ==========================================================
// ARITHMETIC OPERATIONS
// ==========================================================

Value Value::Add(const Value &other) const {
  if (is_null_ || other.is_null_)
    return Value::GetNullValue(type_id_);
  if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
    return Value(TypeId::DECIMAL, CastAsDouble() + other.CastAsDouble());
  if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
    return Value(TypeId::BIGINT, CastAsBigInt() + other.CastAsBigInt());
  return Value(TypeId::INTEGER, CastAsInteger() + other.CastAsInteger());
}

Value Value::Subtract(const Value &other) const {
  if (is_null_ || other.is_null_)
    return Value::GetNullValue(type_id_);
  if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
    return Value(TypeId::DECIMAL, CastAsDouble() - other.CastAsDouble());
  if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
    return Value(TypeId::BIGINT, CastAsBigInt() - other.CastAsBigInt());
  return Value(TypeId::INTEGER, CastAsInteger() - other.CastAsInteger());
}

Value Value::Multiply(const Value &other) const {
  if (is_null_ || other.is_null_)
    return Value::GetNullValue(type_id_);
  if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
    return Value(TypeId::DECIMAL, CastAsDouble() * other.CastAsDouble());
  if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
    return Value(TypeId::BIGINT, CastAsBigInt() * other.CastAsBigInt());
  return Value(TypeId::INTEGER, CastAsInteger() * other.CastAsInteger());
}

Value Value::Divide(const Value &other) const {
  if (is_null_ || other.is_null_)
    return Value::GetNullValue(type_id_);

  // --- M6 FIX: Consolidate division by zero checks safely using native types ---
  bool is_zero = false;
  switch (other.type_id_) {
  case TypeId::TINYINT:  is_zero = (other.val_.tinyint_ == 0); break;
  case TypeId::SMALLINT: is_zero = (other.val_.smallint_ == 0); break;
  case TypeId::INTEGER:  is_zero = (other.val_.integer_ == 0); break;
  case TypeId::BIGINT:   is_zero = (other.val_.bigint_ == 0); break;
  case TypeId::DECIMAL:  is_zero = (other.val_.decimal_ == 0.0); break;
  default: break;
  }
  if (is_zero) throw std::runtime_error("Division by zero.");

  if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
    return Value(TypeId::DECIMAL, CastAsDouble() / other.CastAsDouble());
  if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT) {
    return Value(TypeId::BIGINT, CastAsBigInt() / other.CastAsBigInt());
  }
  return Value(TypeId::INTEGER, CastAsInteger() / other.CastAsInteger());
}

// ==========================================================
// SERIALIZATION
// ==========================================================

uint32_t Value::GetSize() const {
  switch (type_id_) {
  case TypeId::BOOLEAN:
    return 1;
  case TypeId::TINYINT:
    return 1;
  case TypeId::SMALLINT:
    return 2;
  case TypeId::INTEGER:
    return 4;
  case TypeId::BIGINT:
    return 8;
  case TypeId::DECIMAL:
    return 8;
  case TypeId::TIMESTAMP:
    return 8;
  case TypeId::VARCHAR:
  case TypeId::CHAR:
    return 4 + static_cast<uint32_t>(str_value_.length());
  default:
    return 0;
  }
}

uint32_t Value::SerializeTo(char *storage) const {
  switch (type_id_) {
  case TypeId::BOOLEAN:
    memcpy(storage, &val_.boolean_, 1);
    return 1;
  case TypeId::TINYINT:
    memcpy(storage, &val_.tinyint_, 1);
    return 1;
  case TypeId::SMALLINT:
    memcpy(storage, &val_.smallint_, 2);
    return 2;
  case TypeId::INTEGER:
    memcpy(storage, &val_.integer_, 4);
    return 4;
  case TypeId::BIGINT:
    memcpy(storage, &val_.bigint_, 8);
    return 8;
  case TypeId::DECIMAL:
    memcpy(storage, &val_.decimal_, 8);
    return 8;
  case TypeId::TIMESTAMP:
    memcpy(storage, &val_.bigint_, 8);
    return 8;
  case TypeId::VARCHAR:
  case TypeId::CHAR: {
    uint32_t len = static_cast<uint32_t>(str_value_.length());
    memcpy(storage, &len, 4);
    memcpy(storage + 4, str_value_.c_str(), len);
    return 4 + len;
  }
  default:
    return 0;
  }
}

Value Value::DeserializeFrom(const char *storage, TypeId type) {
  switch (type) {
  case TypeId::BOOLEAN: {
    bool b;
    memcpy(&b, storage, 1);
    return Value(type, b);
  }
  case TypeId::TINYINT: {
    int8_t i;
    memcpy(&i, storage, 1);
    return Value(type, i);
  }
  case TypeId::SMALLINT: {
    int16_t i;
    memcpy(&i, storage, 2);
    return Value(type, i);
  }
  case TypeId::INTEGER: {
    int32_t i;
    memcpy(&i, storage, 4);
    return Value(type, i);
  }
  case TypeId::BIGINT: {
    int64_t i;
    memcpy(&i, storage, 8);
    return Value(type, i);
  }
  case TypeId::DECIMAL: {
    double d;
    memcpy(&d, storage, 8);
    return Value(type, d);
  }
  case TypeId::TIMESTAMP: {
    int64_t ts;
    memcpy(&ts, storage, 8);
    return Value(type, ts);
  }
  case TypeId::VARCHAR:
  case TypeId::CHAR: {
    uint32_t len;
    memcpy(&len, storage, 4);
    std::string s(storage + 4, len);
    return Value(type, s);
  }
  default:
    return Value();
  }
}

// ==========================================================
// COMPARISON & DEBUG
// ==========================================================

bool Value::CompareEquals(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;

  // 1. If types match exactly, use fast direct comparison
  if (type_id_ == other.type_id_) {
    switch (type_id_) {
    case TypeId::BOOLEAN:
      return val_.boolean_ == other.val_.boolean_;
    case TypeId::TINYINT:
      return val_.tinyint_ == other.val_.tinyint_;
    case TypeId::SMALLINT:
      return val_.smallint_ == other.val_.smallint_;
    case TypeId::INTEGER:
      return val_.integer_ == other.val_.integer_;
    case TypeId::BIGINT:
      return val_.bigint_ == other.val_.bigint_;
    case TypeId::DECIMAL:
      return val_.decimal_ == other.val_.decimal_;
    case TypeId::TIMESTAMP:
      return val_.bigint_ == other.val_.bigint_;
    case TypeId::VARCHAR:
    case TypeId::CHAR:
      return str_value_ == other.str_value_;
    default:
      return false;
    }
  }

  // 2. If types differ, check if both are numeric and promote them
  if (IsNumeric(type_id_) && IsNumeric(other.type_id_)) {
    if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
      return CastAsDouble() == other.CastAsDouble();
    if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
      return CastAsBigInt() == other.CastAsBigInt();
    return CastAsInteger() == other.CastAsInteger();
  }

  return false;
}

bool Value::CompareNotEquals(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;
  return !CompareEquals(other);
}

bool Value::CompareLessThan(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;

  if (type_id_ == other.type_id_) {
    switch (type_id_) {
    case TypeId::BOOLEAN:
      return val_.boolean_ < other.val_.boolean_;
    case TypeId::TINYINT:
      return val_.tinyint_ < other.val_.tinyint_;
    case TypeId::SMALLINT:
      return val_.smallint_ < other.val_.smallint_;
    case TypeId::INTEGER:
      return val_.integer_ < other.val_.integer_;
    case TypeId::BIGINT:
      return val_.bigint_ < other.val_.bigint_;
    case TypeId::DECIMAL:
      return val_.decimal_ < other.val_.decimal_;
    case TypeId::TIMESTAMP:
      return val_.bigint_ < other.val_.bigint_;
    case TypeId::VARCHAR:
    case TypeId::CHAR:
      return str_value_ < other.str_value_;
    default:
      return false;
    }
  }

  if (IsNumeric(type_id_) && IsNumeric(other.type_id_)) {
    if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
      return CastAsDouble() < other.CastAsDouble();
    if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
      return CastAsBigInt() < other.CastAsBigInt();
    return CastAsInteger() < other.CastAsInteger();
  }

  return false;
}

bool Value::CompareGreaterThan(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;

  if (type_id_ == other.type_id_) {
    switch (type_id_) {
    case TypeId::BOOLEAN:
      return val_.boolean_ > other.val_.boolean_;
    case TypeId::TINYINT:
      return val_.tinyint_ > other.val_.tinyint_;
    case TypeId::SMALLINT:
      return val_.smallint_ > other.val_.smallint_;
    case TypeId::INTEGER:
      return val_.integer_ > other.val_.integer_;
    case TypeId::BIGINT:
      return val_.bigint_ > other.val_.bigint_;
    case TypeId::DECIMAL:
      return val_.decimal_ > other.val_.decimal_;
    case TypeId::TIMESTAMP:
      return val_.bigint_ > other.val_.bigint_;
    case TypeId::VARCHAR:
    case TypeId::CHAR:
      return str_value_ > other.str_value_;
    default:
      return false;
    }
  }

  if (IsNumeric(type_id_) && IsNumeric(other.type_id_)) {
    if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
      return CastAsDouble() > other.CastAsDouble();
    if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
      return CastAsBigInt() > other.CastAsBigInt();
    return CastAsInteger() > other.CastAsInteger();
  }

  return false;
}

static bool MatchLikeStr(const char *str, const char *pattern,
                         bool case_insensitive) {
  // If we reach the end of the pattern, we must also be at the end of the
  // string
  if (*pattern == '\0') {
    return *str == '\0';
  }

  // Handle '%' wildcard: matches zero or more characters
  if (*pattern == '%') {
    // Skip consecutive '%' signs to avoid redundant recursion
    while (*(pattern + 1) == '%') {
      pattern++;
    }

    // Try matching the rest of the pattern with decreasing lengths of the
    // string
    const char *s = str;
    while (true) {
      if (MatchLikeStr(s, pattern + 1, case_insensitive)) {
        return true;
      }
      if (*s == '\0') {
        return false;
      }
      s++;
    }
  }

  // If the string is empty but the pattern isn't (and it's not a '%'), it's a
  // mismatch
  if (*str == '\0') {
    return false;
  }

  // Handle '_' wildcard: matches exactly one arbitrary character
  if (*pattern == '_') {
    return MatchLikeStr(str + 1, pattern + 1, case_insensitive);
  }

  // Handle exact character matches (with optional case-insensitivity)
  bool char_match = false;
  if (case_insensitive) {
    char_match = std::tolower(static_cast<unsigned char>(*str)) ==
                 std::tolower(static_cast<unsigned char>(*pattern));
  } else {
    char_match = (*str == *pattern);
  }

  if (char_match) {
    return MatchLikeStr(str + 1, pattern + 1, case_insensitive);
  }

  return false;
}

bool Value::CompareLike(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;
  if (type_id_ != TypeId::VARCHAR && type_id_ != TypeId::CHAR) {
    throw std::runtime_error(
        "LIKE operator requires a string on the left side.");
  }
  if (other.type_id_ != TypeId::VARCHAR && other.type_id_ != TypeId::CHAR) {
    throw std::runtime_error(
        "LIKE operator requires a string on the right side.");
  }

  return MatchLikeStr(str_value_.c_str(), other.str_value_.c_str(), false);
}

bool Value::CompareILike(const Value &other) const {
  if (is_null_ || other.is_null_)
    return false;
  if (type_id_ != TypeId::VARCHAR && type_id_ != TypeId::CHAR) {
    throw std::runtime_error(
        "ILIKE operator requires a string on the left side.");
  }
  if (other.type_id_ != TypeId::VARCHAR && other.type_id_ != TypeId::CHAR) {
    throw std::runtime_error(
        "ILIKE operator requires a string on the right side.");
  }

  return MatchLikeStr(str_value_.c_str(), other.str_value_.c_str(), true);
}

// ==========================================================
// NATIVE HASHING
// ==========================================================
std::size_t Value::Hash() const {
  std::size_t hash = 0;
  switch (type_id_) {
  case TypeId::BOOLEAN:
    hash = std::hash<bool>()(val_.boolean_);
    break;
  case TypeId::TINYINT:
    hash = std::hash<int8_t>()(val_.tinyint_);
    break;
  case TypeId::SMALLINT:
    hash = std::hash<int16_t>()(val_.smallint_);
    break;
  case TypeId::INTEGER:
    hash = std::hash<int32_t>()(val_.integer_);
    break;
  case TypeId::BIGINT:
    hash = std::hash<int64_t>()(val_.bigint_);
    break;
  case TypeId::DECIMAL:
    hash = std::hash<double>()(val_.decimal_);
    break;
  case TypeId::TIMESTAMP:
    hash = std::hash<int64_t>()(val_.bigint_);
    break;
  case TypeId::VARCHAR:
  case TypeId::CHAR:
    hash = std::hash<std::string>()(str_value_);
    break;
  default:
    hash = 0;
    break;
  }

  // Mix the hash with the TypeId to prevent collisions between different types
  return hash ^ (std::hash<int>()(static_cast<int>(type_id_)) << 1);
}

std::string Value::ToString() const {
  if (is_null_)
    return "NULL";

  switch (type_id_) {
  case TypeId::BOOLEAN:
    return val_.boolean_ ? "true" : "false";
  case TypeId::TINYINT:
    return std::to_string(val_.tinyint_);
  case TypeId::SMALLINT:
    return std::to_string(val_.smallint_);
  case TypeId::INTEGER:
    return std::to_string(val_.integer_);
  case TypeId::BIGINT:
    return std::to_string(val_.bigint_);
  case TypeId::DECIMAL:
    return std::to_string(val_.decimal_);
  case TypeId::TIMESTAMP: {
    time_t t = static_cast<time_t>(val_.bigint_);
    struct std::tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
    return oss.str();
  }
  case TypeId::VARCHAR:
  case TypeId::CHAR:
    return str_value_;
  default:
    return "Invalid";
  }
}

} // namespace tetodb