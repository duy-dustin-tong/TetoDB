#pragma once

#include <vector>
#include <cstring>
#include <string>

#include "common/record_id.h"
#include "catalog/schema.h" // Needed to understand data layout
#include "type/value.h"     // Needed to return data

namespace tetodb {

    class Tuple {
    public:
        // 1. Default Constructor (Empty)
        Tuple() = default;

        // 2. CONSTRUCTOR: Raw Bytes (Disk -> Memory)
        // Used by TableHeap when reading from a page.
        Tuple(RID rid, const char* data, uint32_t size)
            : rid_(rid) {
            if (size > 0) {
                data_.resize(size);
                memcpy(data_.data(), data, size);
            }
        }

        // 3. CONSTRUCTOR: Values -> Bytes (Memory -> Disk)
        // [NEW] Used by Executor when inserting new rows.
        Tuple(const std::vector<Value>& values, const Schema* schema);

        // 4. ACCESSOR: Bytes -> Value
        // [NEW] Reads a specific column using the schema.
        Value GetValue(const Schema* schema, uint32_t col_idx) const;

        // --- Getters ---
        inline RID GetRid() const { return rid_; }
        inline void SetRid(RID rid) { rid_ = rid; }
        inline const char* GetData() const { return data_.data(); }
        inline uint32_t GetSize() const { return static_cast<uint32_t>(data_.size()); }

        // --- Utilities ---
        void SerializeTo(char* storage) const {
            memcpy(storage, data_.data(), data_.size());
        }

        void DeserializeFrom(const char* storage, uint32_t size) {
            data_.resize(size);
            std::memcpy(data_.data(), storage, size);
        }

        // [UPDATED] formatted print using schema
        std::string ToString(const Schema* schema) const;

    private:
        RID rid_;
        std::vector<char> data_; // The physical payload
    };

} // namespace tetodb