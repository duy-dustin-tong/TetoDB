// tuple.cpp

#include "storage/table/tuple.h"
#include "type/type.h"
#include <sstream>
#include <algorithm> // for std::min

namespace tetodb {

    // --- CONSTRUCTOR: Serializes Values -> Bytes ---
    Tuple::Tuple(const std::vector<Value>& values, const Schema* schema) {
        // 1. Calculate Sizes
        // The "Header" size is the sum of all fixed lengths (INT=4, CHAR(10)=10, VARCHAR=4)
        uint32_t header_size = schema->GetLength();
        uint32_t heap_size = 0;

        // Calculate how much extra space we need for the Heap (VARCHAR data)
        for (const auto& val : values) {
            if (val.GetTypeId() == TypeId::VARCHAR) {
                heap_size += val.GetSize();
            }
        }

        // 2. Allocate Total Memory
        // Size = Header + Heap
        data_.resize(header_size + heap_size);

        // 3. Setup Pointers
        char* header_ptr = data_.data();              // Start of Tuple

        // Offset relative to the start of the tuple where the HEAP begins
        uint32_t current_heap_offset = header_size;

        // 4. Write Data
        for (uint32_t i = 0; i < values.size(); i++) {
            const Column& col = schema->GetColumn(i);
            const Value& val = values[i];

            // 1. Get the Type Handler
            const Type* type_handler = Type::GetInstance(col.GetTypeId());

            if (col.IsInlined()) {
                // FIXED TYPES (INT, CHAR)
                // Polymorphism handles the details (Padding vs Raw Copy)
                type_handler->SerializeTo(val, header_ptr + col.GetOffset(), col.GetFixedLength());
            }
            else {
                // VARIABLE TYPES (VARCHAR)
                // 1. Write Offset (Tuple handles this structure)
                *reinterpret_cast<uint32_t*>(header_ptr + col.GetOffset()) = current_heap_offset;

                // 2. Write Data to Heap (Type handler writes Length + Bytes)
                uint32_t written = type_handler->SerializeToHeap(val, data_.data() + current_heap_offset);
                current_heap_offset += written;
            }
        }
    }

    // --- ACCESSOR: Deserializes Bytes -> Values ---
    Value Tuple::GetValue(const Schema* schema, uint32_t col_idx) const {
        const Column& col = schema->GetColumn(col_idx);
        const char* ptr = data_.data() + col.GetOffset();

        const Type* type_handler = Type::GetInstance(col.GetTypeId());

        if (col.IsInlined()) {
            // FIXED TYPES
            // Polymorphism handles reading padding/nulls
            return type_handler->DeserializeFrom(ptr, col.GetFixedLength());
        }
        else {
            // VARIABLE TYPES
            uint32_t var_offset = *reinterpret_cast<const uint32_t*>(ptr);
            const char* var_ptr = data_.data() + var_offset;

            // For now, Varchar still relies on Value::DeserializeFrom for heap structure
            return Value::DeserializeFrom(var_ptr, col.GetTypeId());
        }
    }

    std::string Tuple::ToString(const Schema* schema) const {
        std::stringstream os;
        os << "(";
        for (uint32_t i = 0; i < schema->GetColumns().size(); i++) {
            os << GetValue(schema, i).ToString();
            if (i < schema->GetColumns().size() - 1) os << ", ";
        }
        os << ")";
        return os.str();
    }

} // namespace tetodb