// value.h

#pragma once

#include <string>
#include <vector>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include <algorithm> // for std::max

#include "type/type_id.h"

namespace tetodb {

    class Value {
    public:
        // --- Constructors ---

        // 1. Invalid / Null
        Value() : type_id_(TypeId::INVALID) {
            memset(&val_, 0, sizeof(Val)); // Zero out everything
        }

        // 2. Integers (Tiny, Small, Int, Big)
        Value(TypeId type, int32_t i) : type_id_(type) {
            memset(&val_, 0, sizeof(Val)); // Zero out first!
            val_.integer_ = i;
        }

        Value(TypeId type, int64_t i) : type_id_(type) {
            memset(&val_, 0, sizeof(Val));
            val_.bigint_ = i;
        }

        // 3. Decimals
        Value(TypeId type, double d) : type_id_(type) {
            memset(&val_, 0, sizeof(Val));
            val_.decimal_ = d;
        }

        // 4. Booleans
        Value(TypeId type, bool b) : type_id_(type) {
            memset(&val_, 0, sizeof(Val));
            val_.boolean_ = b;
        }

        // 5. Strings (Varchar)
        Value(TypeId type, std::string s) : type_id_(type), str_value_(std::move(s)) {
            // In a pro DB, we would enforce length limits here based on schema
        }

        Value(TypeId type, const char* s) : type_id_(type), str_value_(s) {
            // Now "Hi" goes here, not to bool!
        }

        // --- Copy / Move Semantics ---
        Value(const Value& other) : type_id_(other.type_id_), str_value_(other.str_value_) {
            val_ = other.val_; // Bitwise copy of the union
        }

        Value& operator=(const Value& other) {
            if (this != &other) {
                type_id_ = other.type_id_;
                val_ = other.val_;
                str_value_ = other.str_value_;
            }
            return *this;
        }

        // --- Accessors ---
        inline TypeId GetTypeId() const { return type_id_; }

        // Fast accessors (Use these when you KNOW the type)
        inline int32_t GetAsInteger() const { return val_.integer_; }
        inline int64_t GetAsBigInt() const { return val_.bigint_; }
        inline double GetAsDecimal() const { return val_.decimal_; }
        inline bool GetAsBoolean() const { return val_.boolean_; }
        inline std::string GetAsString() const { return str_value_; }

        // --- Operations ---
        // These will be used by the Execution Engine (Filters, Joins)
        bool CompareEquals(const Value& other) const;
        bool CompareNotEquals(const Value& other) const;
        bool CompareLessThan(const Value& other) const;
        bool CompareGreaterThan(const Value& other) const;

        // Debugging
        std::string ToString() const;

        // --- Disk Storage (Serialization) ---
        // Writes the value's bytes to 'storage'. Returns bytes written.
        uint32_t SerializeTo(char* storage) const;

        // Reads a value from 'storage'. Returns bytes read.
        // NOTE: We need the TypeId to know how many bytes to read!
        static Value DeserializeFrom(const char* storage, TypeId type);

        // How many bytes does this occupy on disk?
        uint32_t GetSize() const;

    private:
        TypeId type_id_;

        // UNION OPTIMIZATION:
        // All these primitives share the same 8 bytes of memory.
        union Val {
            int8_t tinyint_;
            int16_t smallint_;
            int32_t integer_;
            int64_t bigint_;
            double decimal_;
            bool boolean_;
        } val_;

        // Separate storage for strings (Safe RAII)
        std::string str_value_;
    };

} // namespace tetodb