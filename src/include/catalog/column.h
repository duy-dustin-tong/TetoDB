// column.h

#pragma once

#include <string>
#include <utility>
#include <iostream>
#include "type/type_id.h"

namespace tetodb {

    class Column {
    public:
        // --- Constructor 1: Standard TypeIds (INT, BOOL, BIGINT) ---
        // Usage: Column("id", TypeId::INTEGER)
        Column(std::string name, TypeId type)
            : name_(std::move(name)), type_id_(type) {

            switch (type) {
            case TypeId::BOOLEAN:     fixed_length_ = 1; break;
            case TypeId::TINYINT:     fixed_length_ = 1; break;
            case TypeId::SMALLINT:    fixed_length_ = 2; break;
            case TypeId::INTEGER:     fixed_length_ = 4; break;
            case TypeId::BIGINT:      fixed_length_ = 8; break;
            case TypeId::DECIMAL:     fixed_length_ = 8; break;
            case TypeId::TIMESTAMP:   fixed_length_ = 8; break;

                // If user forgets length for VARCHAR, we default to 4-byte offset + 255 limit
            case TypeId::VARCHAR:
                fixed_length_ = 4; // Offset Pointer size
                variable_limit_ = 255;
                break;

                // If user forgets length for CHAR, we default to 1 byte
            case TypeId::CHAR:
                fixed_length_ = 1;
                break;

            default: fixed_length_ = 0; break;
            }
        }

        // --- Constructor 2: Parameterized TypeIds (VARCHAR(N), CHAR(N)) ---
        // Usage: Column("username", TypeId::VARCHAR, 50)
        Column(std::string name, TypeId type, uint32_t length_limit)
            : name_(std::move(name)), type_id_(type) {

            if (type == TypeId::VARCHAR) {
                // VARCHAR stores a 4-byte offset in the header.
                // The 'length_limit' is just a constraint (e.g., max 50 chars).
                fixed_length_ = 4;
                variable_limit_ = length_limit;
            }
            else if (type == TypeId::CHAR) {
                // CHAR stores the data DIRECTLY in the header.
                // fixed_length_ IS the length limit.
                fixed_length_ = length_limit;
                variable_limit_ = 0; // Not variable
            }
            else {
                // Fallback for standard types if someone calls this constructor by mistake
                // e.g. Column("id", TypeId::INTEGER, 999) -> ignores 999
                switch (type) {
                case TypeId::BOOLEAN:     fixed_length_ = 1; break;
                case TypeId::TINYINT:     fixed_length_ = 1; break;
                case TypeId::SMALLINT:    fixed_length_ = 2; break;
                case TypeId::INTEGER:     fixed_length_ = 4; break;
                case TypeId::BIGINT:      fixed_length_ = 8; break;
                case TypeId::DECIMAL:     fixed_length_ = 8; break;
                case TypeId::TIMESTAMP:   fixed_length_ = 8; break;
                default:                fixed_length_ = 0; break;
                }
            }
        }

        // --- Getters (Inlined for O(1) Access) ---

        inline std::string GetName() const { return name_; }
        inline TypeId GetTypeId() const { return type_id_; }

        // Returns the space occupied in the Fixed Tuple Header.
        // For INT = 4, CHAR(10) = 10, VARCHAR = 4 (Pointer).
        inline uint32_t GetFixedLength() const { return fixed_length_; }

        // Returns the max length constraint (Only relevant for VARCHAR).
        inline uint32_t GetStorageLimit() const { return variable_limit_; }

        // Returns the byte offset of this column within the tuple header.
        inline uint32_t GetOffset() const { return offset_; }
        inline void SetOffset(uint32_t offset) { offset_ = offset; }

        // Returns true if the data is stored directly in the header.
        // CHAR is Inlined. VARCHAR is NOT Inlined (stored in Heap).
        inline bool IsInlined() const { return type_id_ != TypeId::VARCHAR; }

        std::string ToString() const {
            return name_ + ":" + std::to_string(static_cast<int>(type_id_));
        }

    private:
        std::string name_;
        TypeId type_id_;

        // The size this column takes up in the main tuple layout.
        uint32_t fixed_length_{ 0 };

        // For VARCHAR, this is the constraint (e.g. 255).
        // For Fixed types, this is 0.
        uint32_t variable_limit_{ 0 };

        // Calculated by Schema class
        uint32_t offset_{ 0 };
    };

} // namespace tetodb