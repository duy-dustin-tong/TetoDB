// tuple.cpp

#include "storage/table/tuple.h"
#include "type/type.h"
#include <sstream>
#include <algorithm> // for std::min

namespace tetodb {

    // --- CONSTRUCTOR: Serializes Values -> Bytes ---
    Tuple::Tuple(const std::vector<Value>& values, const Schema* schema) {
        uint32_t header_size = schema->GetLength();
        uint32_t heap_size = 0;

        for (const auto& val : values) {
            if (!val.IsNull() && val.GetTypeId() == TypeId::VARCHAR) {
                heap_size += val.GetSize();
            }
        }

        data_.resize(header_size + heap_size);
        char* header_ptr = data_.data();
        uint32_t current_heap_offset = header_size;

        // --- NEW: Zero out the Null Bitmap area first ---
        std::memset(header_ptr, 0, schema->GetBitmapSize());

        for (uint32_t i = 0; i < values.size(); i++) {
            const Column& col = schema->GetColumn(i);
            const Value& val = values[i];

            // --- NEW: Set Null Bit and skip data write ---
            if (val.IsNull()) {
                uint32_t byte_idx = i / 8;
                uint32_t bit_idx = i % 8;
                header_ptr[byte_idx] |= (1 << bit_idx);
                continue;
            }

            const Type* type_handler = Type::GetInstance(col.GetTypeId());

            if (col.IsInlined()) {
                type_handler->SerializeTo(val, header_ptr + col.GetOffset(), col.GetFixedLength());
            }
            else {
                *reinterpret_cast<uint32_t*>(header_ptr + col.GetOffset()) = current_heap_offset;
                uint32_t written = type_handler->SerializeToHeap(val, data_.data() + current_heap_offset);
                current_heap_offset += written;
            }
        }
    }
    // --- ACCESSOR: Deserializes Bytes -> Values ---
    Value Tuple::GetValue(const Schema* schema, uint32_t col_idx) const {
        // --- NEW: Read Null Bitmap ---
        const char* header_ptr = data_.data();
        uint32_t byte_idx = col_idx / 8;
        uint32_t bit_idx = col_idx % 8;

        bool is_null = (header_ptr[byte_idx] & (1 << bit_idx)) != 0;
        if (is_null) {
            return Value::GetNullValue(schema->GetColumn(col_idx).GetTypeId());
        }

        // --- Existing data read logic ---
        const Column& col = schema->GetColumn(col_idx);
        const char* ptr = data_.data() + col.GetOffset();
        const Type* type_handler = Type::GetInstance(col.GetTypeId());

        if (col.IsInlined()) {
            return type_handler->DeserializeFrom(ptr, col.GetFixedLength());
        }
        else {
            uint32_t var_offset = *reinterpret_cast<const uint32_t*>(ptr);
            const char* var_ptr = data_.data() + var_offset;
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