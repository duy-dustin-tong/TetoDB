// value.cpp

#include "type/value.h"
#include <stdexcept>

namespace tetodb {


    // ==========================================================
    // NUMERIC PROMOTION & CASTING
    // ==========================================================

    double Value::CastAsDouble() const {
        switch (type_id_) {
        case TypeId::TINYINT:  return static_cast<double>(val_.tinyint_);
        case TypeId::SMALLINT: return static_cast<double>(val_.smallint_);
        case TypeId::INTEGER:  return static_cast<double>(val_.integer_);
        case TypeId::BIGINT:   return static_cast<double>(val_.bigint_);
        case TypeId::DECIMAL:  return val_.decimal_;
        default: throw std::runtime_error("Type error: Cannot cast non-numeric to double.");
        }
    }

    int64_t Value::CastAsBigInt() const {
        switch (type_id_) {
        case TypeId::TINYINT:  return static_cast<int64_t>(val_.tinyint_);
        case TypeId::SMALLINT: return static_cast<int64_t>(val_.smallint_);
        case TypeId::INTEGER:  return static_cast<int64_t>(val_.integer_);
        case TypeId::BIGINT:   return val_.bigint_;
        case TypeId::DECIMAL:  return static_cast<int64_t>(val_.decimal_);
        default: throw std::runtime_error("Type error: Cannot cast non-numeric to BigInt.");
        }
    }

    int32_t Value::CastAsInteger() const {
        switch (type_id_) {
        case TypeId::TINYINT:  return static_cast<int32_t>(val_.tinyint_);
        case TypeId::SMALLINT: return static_cast<int32_t>(val_.smallint_);
        case TypeId::INTEGER:  return val_.integer_;
        case TypeId::BIGINT:   return static_cast<int32_t>(val_.bigint_);
        case TypeId::DECIMAL:  return static_cast<int32_t>(val_.decimal_);
        default: throw std::runtime_error("Type error: Cannot cast non-numeric to Integer.");
        }
    }

    // ==========================================================
    // ARITHMETIC OPERATIONS
    // ==========================================================

    Value Value::Add(const Value& other) const {
        if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
            return Value(TypeId::DECIMAL, CastAsDouble() + other.CastAsDouble());
        if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
            return Value(TypeId::BIGINT, CastAsBigInt() + other.CastAsBigInt());
        return Value(TypeId::INTEGER, CastAsInteger() + other.CastAsInteger());
    }

    Value Value::Subtract(const Value& other) const {
        if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
            return Value(TypeId::DECIMAL, CastAsDouble() - other.CastAsDouble());
        if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
            return Value(TypeId::BIGINT, CastAsBigInt() - other.CastAsBigInt());
        return Value(TypeId::INTEGER, CastAsInteger() - other.CastAsInteger());
    }

    Value Value::Multiply(const Value& other) const {
        if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
            return Value(TypeId::DECIMAL, CastAsDouble() * other.CastAsDouble());
        if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
            return Value(TypeId::BIGINT, CastAsBigInt() * other.CastAsBigInt());
        return Value(TypeId::INTEGER, CastAsInteger() * other.CastAsInteger());
    }

    Value Value::Divide(const Value& other) const {
        double divisor = other.CastAsDouble();
        if (divisor == 0) throw std::runtime_error("Division by zero.");

        if (type_id_ == TypeId::DECIMAL || other.type_id_ == TypeId::DECIMAL)
            return Value(TypeId::DECIMAL, CastAsDouble() / divisor);
        if (type_id_ == TypeId::BIGINT || other.type_id_ == TypeId::BIGINT)
            return Value(TypeId::BIGINT, CastAsBigInt() / other.CastAsBigInt());
        return Value(TypeId::INTEGER, CastAsInteger() / other.CastAsInteger());
    }

    // ==========================================================
    // SERIALIZATION
    // ==========================================================

    uint32_t Value::GetSize() const {
        switch (type_id_) {
        case TypeId::BOOLEAN:  return 1;
        case TypeId::TINYINT:  return 1;
        case TypeId::SMALLINT: return 2;
        case TypeId::INTEGER:  return 4;
        case TypeId::BIGINT:   return 8;
        case TypeId::DECIMAL:  return 8;
        case TypeId::VARCHAR:
        case TypeId::CHAR:     return 4 + static_cast<uint32_t>(str_value_.length());
        default: return 0;
        }
    }

    uint32_t Value::SerializeTo(char* storage) const {
        switch (type_id_) {
        case TypeId::BOOLEAN:  memcpy(storage, &val_.boolean_, 1);  return 1;
        case TypeId::TINYINT:  memcpy(storage, &val_.tinyint_, 1);  return 1;
        case TypeId::SMALLINT: memcpy(storage, &val_.smallint_, 2); return 2;
        case TypeId::INTEGER:  memcpy(storage, &val_.integer_, 4);  return 4;
        case TypeId::BIGINT:   memcpy(storage, &val_.bigint_, 8);   return 8;
        case TypeId::DECIMAL:  memcpy(storage, &val_.decimal_, 8);  return 8;
        case TypeId::VARCHAR:
        case TypeId::CHAR: {
            uint32_t len = static_cast<uint32_t>(str_value_.length());
            memcpy(storage, &len, 4);
            memcpy(storage + 4, str_value_.c_str(), len);
            return 4 + len;
        }
        default: return 0;
        }
    }

    Value Value::DeserializeFrom(const char* storage, TypeId type) {
        switch (type) {
        case TypeId::BOOLEAN: {
            bool b; memcpy(&b, storage, 1);
            return Value(type, b);
        }
        case TypeId::TINYINT: {
            int8_t i; memcpy(&i, storage, 1);
            return Value(type, i);
        }
        case TypeId::SMALLINT: {
            int16_t i; memcpy(&i, storage, 2);
            return Value(type, i);
        }
        case TypeId::INTEGER: {
            int32_t i; memcpy(&i, storage, 4);
            return Value(type, i);
        }
        case TypeId::BIGINT: {
            int64_t i; memcpy(&i, storage, 8);
            return Value(type, i);
        }
        case TypeId::DECIMAL: {
            double d; memcpy(&d, storage, 8);
            return Value(type, d);
        }
        case TypeId::VARCHAR:
        case TypeId::CHAR: {
            uint32_t len; memcpy(&len, storage, 4);
            std::string s(storage + 4, len);
            return Value(type, s);
        }
        default: return Value();
        }
    }

    // ==========================================================
    // COMPARISON & DEBUG
    // ==========================================================

    bool Value::CompareEquals(const Value& other) const {
        // 1. If types match exactly, use fast direct comparison
        if (type_id_ == other.type_id_) {
            switch (type_id_) {
            case TypeId::BOOLEAN:  return val_.boolean_ == other.val_.boolean_;
            case TypeId::TINYINT:  return val_.tinyint_ == other.val_.tinyint_;
            case TypeId::SMALLINT: return val_.smallint_ == other.val_.smallint_;
            case TypeId::INTEGER:  return val_.integer_ == other.val_.integer_;
            case TypeId::BIGINT:   return val_.bigint_ == other.val_.bigint_;
            case TypeId::DECIMAL:  return val_.decimal_ == other.val_.decimal_;
            case TypeId::VARCHAR:
            case TypeId::CHAR:     return str_value_ == other.str_value_;
            default: return false;
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

    bool Value::CompareNotEquals(const Value& other) const {
        return !CompareEquals(other);
    }

    bool Value::CompareLessThan(const Value& other) const {
        if (type_id_ == other.type_id_) {
            switch (type_id_) {
            case TypeId::BOOLEAN:  return val_.boolean_ < other.val_.boolean_;
            case TypeId::TINYINT:  return val_.tinyint_ < other.val_.tinyint_;
            case TypeId::SMALLINT: return val_.smallint_ < other.val_.smallint_;
            case TypeId::INTEGER:  return val_.integer_ < other.val_.integer_;
            case TypeId::BIGINT:   return val_.bigint_ < other.val_.bigint_;
            case TypeId::DECIMAL:  return val_.decimal_ < other.val_.decimal_;
            case TypeId::VARCHAR:
            case TypeId::CHAR:     return str_value_ < other.str_value_;
            default: return false;
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

    bool Value::CompareGreaterThan(const Value& other) const {
        if (type_id_ == other.type_id_) {
            switch (type_id_) {
            case TypeId::BOOLEAN:  return val_.boolean_ > other.val_.boolean_;
            case TypeId::TINYINT:  return val_.tinyint_ > other.val_.tinyint_;
            case TypeId::SMALLINT: return val_.smallint_ > other.val_.smallint_;
            case TypeId::INTEGER:  return val_.integer_ > other.val_.integer_;
            case TypeId::BIGINT:   return val_.bigint_ > other.val_.bigint_;
            case TypeId::DECIMAL:  return val_.decimal_ > other.val_.decimal_;
            case TypeId::VARCHAR:
            case TypeId::CHAR:     return str_value_ > other.str_value_;
            default: return false;
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

    // ==========================================================
    // NATIVE HASHING
    // ==========================================================
    std::size_t Value::Hash() const {
        std::size_t hash = 0;
        switch (type_id_) {
        case TypeId::BOOLEAN:  hash = std::hash<bool>()(val_.boolean_); break;
        case TypeId::TINYINT:  hash = std::hash<int8_t>()(val_.tinyint_); break;
        case TypeId::SMALLINT: hash = std::hash<int16_t>()(val_.smallint_); break;
        case TypeId::INTEGER:  hash = std::hash<int32_t>()(val_.integer_); break;
        case TypeId::BIGINT:   hash = std::hash<int64_t>()(val_.bigint_); break;
        case TypeId::DECIMAL:  hash = std::hash<double>()(val_.decimal_); break;
        case TypeId::VARCHAR:
        case TypeId::CHAR:     hash = std::hash<std::string>()(str_value_); break;
        default:               hash = 0; break;
        }

        // Mix the hash with the TypeId to prevent collisions between different types
        return hash ^ (std::hash<int>()(static_cast<int>(type_id_)) << 1);
    }

    std::string Value::ToString() const {
        if (is_null_) return "NULL";

        switch (type_id_) {
        case TypeId::BOOLEAN:  return val_.boolean_ ? "true" : "false";
        case TypeId::TINYINT:  return std::to_string(val_.tinyint_);
        case TypeId::SMALLINT: return std::to_string(val_.smallint_);
        case TypeId::INTEGER:  return std::to_string(val_.integer_);
        case TypeId::BIGINT:   return std::to_string(val_.bigint_);
        case TypeId::DECIMAL:  return std::to_string(val_.decimal_);
        case TypeId::VARCHAR:
        case TypeId::CHAR:     return str_value_;
        default: return "Invalid";
        }
    }

} // namespace tetodb