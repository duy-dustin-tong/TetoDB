// src/type/value.cpp

#include "type/value.h"

namespace tetodb {

    // --- Serialization ---

    uint32_t Value::GetSize() const {
        switch (type_id_) {
        case TypeId::BOOLEAN: return 1;
        case TypeId::TINYINT: return 1;
        case TypeId::SMALLINT: return 2;
        case TypeId::INTEGER: return 4;
        case TypeId::BIGINT: return 8;
        case TypeId::DECIMAL: return 8;
        case TypeId::VARCHAR:
            // 4 bytes for Length + N bytes for data
            return 4 + static_cast<uint32_t>(str_value_.length());
        case TypeId::CHAR:
            // For the Value class (in memory), we treat CHAR like VARCHAR.
            // (The strict padding to N bytes happens in the Tuple class).
            return 4 + static_cast<uint32_t>(str_value_.length());
        default: return 0;
        }
    }

    uint32_t Value::SerializeTo(char* storage) const {
        switch (type_id_) {
        case TypeId::BOOLEAN: {
            memcpy(storage, &val_.boolean_, 1);
            return 1;
        }
        case TypeId::TINYINT: {
            memcpy(storage, &val_.tinyint_, 1);
            return 1;
        }
        case TypeId::SMALLINT: {
            memcpy(storage, &val_.smallint_, 2);
            return 2;
        }
        case TypeId::INTEGER: {
            memcpy(storage, &val_.integer_, 4);
            return 4;
        }
        case TypeId::BIGINT: {
            memcpy(storage, &val_.bigint_, 8);
            return 8;
        }
        case TypeId::DECIMAL: {
            memcpy(storage, &val_.decimal_, 8);
            return 8;
        }
        case TypeId::VARCHAR: {
            uint32_t len = static_cast<uint32_t>(str_value_.length());
            memcpy(storage, &len, sizeof(uint32_t));
            memcpy(storage + sizeof(uint32_t), str_value_.c_str(), len);
            return sizeof(uint32_t) + len;
        }
        case TypeId::CHAR: {
            // Same serialization as VARCHAR for intermediate storage
            uint32_t len = static_cast<uint32_t>(str_value_.length());
            memcpy(storage, &len, sizeof(uint32_t));
            memcpy(storage + sizeof(uint32_t), str_value_.c_str(), len);
            return sizeof(uint32_t) + len;
        }
        default: return 0;
        }
    }

    Value Value::DeserializeFrom(const char* storage, TypeId type) {
        switch (type) {
        case TypeId::BOOLEAN: {
            bool val;
            memcpy(&val, storage, 1);
            return Value(TypeId::BOOLEAN, val);
        }
        case TypeId::TINYINT: {
            int8_t val;
            memcpy(&val, storage, 1);
            return Value(TypeId::TINYINT, static_cast<int32_t>(val));
        }
        case TypeId::SMALLINT: {
            int16_t val;
            memcpy(&val, storage, 2);
            return Value(TypeId::SMALLINT, static_cast<int32_t>(val));
        }
        case TypeId::INTEGER: {
            int32_t val;
            memcpy(&val, storage, 4);
            return Value(TypeId::INTEGER, val);
        }
        case TypeId::BIGINT: {
            int64_t val;
            memcpy(&val, storage, 8);
            return Value(TypeId::BIGINT, val);
        }
        case TypeId::DECIMAL: {
            double val;
            memcpy(&val, storage, 8);
            return Value(TypeId::DECIMAL, val);
        }
        case TypeId::VARCHAR: {
            uint32_t len;
            memcpy(&len, storage, 4);
            std::string s;
            s.resize(len);
            memcpy(s.data(), storage + 4, len);
            return Value(TypeId::VARCHAR, s);
        }
        case TypeId::CHAR: {
            uint32_t len;
            memcpy(&len, storage, 4);
            std::string s;
            s.resize(len);
            memcpy(s.data(), storage + 4, len);
            return Value(TypeId::CHAR, s);
        }
        default: return Value();
        }
    }

    // --- Comparison Logic ---
    bool Value::CompareEquals(const Value& other) const {
        if (type_id_ != other.type_id_) return false;

        switch (type_id_) {
        case TypeId::BOOLEAN: return val_.boolean_ == other.val_.boolean_;
        case TypeId::TINYINT: return val_.tinyint_ == other.val_.tinyint_;
        case TypeId::SMALLINT: return val_.smallint_ == other.val_.smallint_;
        case TypeId::INTEGER: return val_.integer_ == other.val_.integer_;
        case TypeId::BIGINT: return val_.bigint_ == other.val_.bigint_;
        case TypeId::DECIMAL: return val_.decimal_ == other.val_.decimal_;
        case TypeId::VARCHAR: return str_value_ == other.str_value_;
        case TypeId::CHAR:    return str_value_ == other.str_value_; 
        default: return false;
        }
    }

    // --- Debug ---
    std::string Value::ToString() const {
        switch (type_id_) {
        case TypeId::BOOLEAN: return val_.boolean_ ? "true" : "false";
        case TypeId::TINYINT: return std::to_string(val_.tinyint_);
        case TypeId::SMALLINT: return std::to_string(val_.smallint_);
        case TypeId::INTEGER: return std::to_string(val_.integer_);
        case TypeId::BIGINT: return std::to_string(val_.bigint_);
        case TypeId::DECIMAL: return std::to_string(val_.decimal_);
        case TypeId::VARCHAR: return str_value_;
        case TypeId::CHAR:    return str_value_; // <--- Added CHAR
        default: return "Invalid";
        }
    }

} // namespace tetodb