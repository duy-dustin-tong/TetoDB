// type.cpp

#include "type/type.h"
#include <stdexcept>
#include <cstring>
#include <algorithm>

namespace tetodb {

    // =========================================================================
    // 1. Number / Primitive Type
    // =========================================================================
    class NumberType : public Type {
    public:
        explicit NumberType(TypeId type_id) : Type(type_id) {}

        uint32_t GetFixedLength() const override {
            switch (type_id_) {
            case TypeId::BOOLEAN: return 1;
            case TypeId::TINYINT: return 1;
            case TypeId::SMALLINT: return 2;
            case TypeId::INTEGER: return 4;
            case TypeId::BIGINT: return 8;
            case TypeId::DECIMAL: return 8;
            case TypeId::TIMESTAMP: return 8;
            default: return 0;
            }
        }

        bool IsInlined() const override { return true; }

        void SerializeTo(const Value& val, char* storage, uint32_t max_length) const override {
            // Primitive types don't care about max_length (it's fixed by definition)
            val.SerializeTo(storage);
        }

        Value DeserializeFrom(const char* storage, uint32_t max_length) const override {
            return Value::DeserializeFrom(storage, type_id_);
        }
    };

    // =========================================================================
    // 2. Char Type (Fixed Length String)
    // =========================================================================
    class CharType : public Type {
    public:
        CharType() : Type(TypeId::CHAR) {}

        uint32_t GetFixedLength() const override { return 0; } // It varies per column definition
        bool IsInlined() const override { return true; }

        void SerializeTo(const Value& val, char* storage, uint32_t max_length) const override {
            // THIS is where the padding logic lives now!
            
            std::string s = val.GetAsString();
            uint32_t len = static_cast<uint32_t>(s.length());
            uint32_t copy_len = std::min(len, max_length);

            // 1. Write Raw String Data
            memcpy(storage, s.c_str(), copy_len);

            // 2. Write Zero Padding (if string is shorter than CHAR(N))
            if (copy_len < max_length) {
                memset(storage + copy_len, 0, max_length - copy_len);
            }
        }

        Value DeserializeFrom(const char* storage, uint32_t max_length) const override {
            // Optimization: Use memchr to find the null terminator. 
            // This is often SIMD-optimized by the standard library (checks 16-32 bytes at once).
            const void* null_ptr = std::memchr(storage, '\0', max_length);

            uint32_t actual_len;
            if (null_ptr == nullptr) {
                // Case A: The string fills the entire CHAR(N) slot (No null terminator found).
                actual_len = max_length;
            }
            else {
                // Case B: Found a null terminator. Calculate length via pointer math.
                actual_len = static_cast<uint32_t>(static_cast<const char*>(null_ptr) - storage);
            }

            // Construct the string with the exact length (avoiding reallocation/trimming later)
            return Value(TypeId::CHAR, std::string(storage, actual_len));
        }
    };

    // =========================================================================
    // 3. Varchar Type (Variable Length)
    // =========================================================================
    class VarcharType : public Type {
    public:
        VarcharType() : Type(TypeId::VARCHAR) {}

        uint32_t GetFixedLength() const override { return 4; } // The Pointer size
        bool IsInlined() const override { return false; }      // Stores data in Heap

        void SerializeTo(const Value& val, char* storage, uint32_t max_length) const override {
            // We do NOT write the string here. We only write the OFFSET pointer.
            // This function is called by Tuple to write the Header.
            // But wait... Tuple handles the offset calculation manually.
            // So this method effectively does nothing for Varchar in the Header.
            // (The Tuple logic manages the 4-byte offset write).
            throw std::runtime_error("Varchar SerializeTo should not be called for Header writing directly.");
        }

        Value DeserializeFrom(const char* storage, uint32_t max_length) const override {
            // Tuple handles the pointer jump. Logic delegates to Value class.
            throw std::runtime_error("Use Tuple::GetValue for Varchar.");
        }

        uint32_t SerializeToHeap(const Value& val, char* storage) const override {
            return val.SerializeTo(storage); // Write Length + Data
        }
    };

    // =========================================================================
    // Singleton Registry
    // =========================================================================

    // Static instances
    static NumberType kTinyInt(TypeId::TINYINT);
    static NumberType kSmallInt(TypeId::SMALLINT);
    static NumberType kInteger(TypeId::INTEGER);
    static NumberType kBigInt(TypeId::BIGINT);
    static NumberType kDecimal(TypeId::DECIMAL);
    static NumberType kBoolean(TypeId::BOOLEAN);
    static NumberType kTimestamp(TypeId::TIMESTAMP);
    static CharType    kChar;
    static VarcharType kVarchar;

    const Type* Type::GetInstance(TypeId type_id) {
        switch (type_id) {
        case TypeId::TINYINT: return &kTinyInt;
        case TypeId::SMALLINT: return &kSmallInt;
        case TypeId::INTEGER: return &kInteger;
        case TypeId::BIGINT: return &kBigInt;
        case TypeId::DECIMAL: return &kDecimal;
        case TypeId::TIMESTAMP: return &kTimestamp;
        case TypeId::BOOLEAN: return &kBoolean;
        case TypeId::CHAR:    return &kChar;
        case TypeId::VARCHAR: return &kVarchar;
        default: throw std::runtime_error("Unknown Type ID");
        }
    }

} // namespace tetodb