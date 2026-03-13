// schema.h

#pragma once

#include <vector>
#include <string>
#include <sstream>
#include "catalog/column.h"

namespace tetodb {

    class Schema {
    public:
        // --- Constructor ---
        // Takes a list of columns and calculates the physical memory layout (Offsets).
        explicit Schema(const std::vector<Column>& columns) : columns_(columns) {
            // 1. Calculate Null Bitmap Size (1 bit per column, rounded up to nearest byte)
            bitmap_size_ = (columns_.size() + 7) / 8;

            // 2. Start data offsets AFTER the bitmap
            uint32_t current_offset = bitmap_size_;

            for (uint32_t i = 0; i < columns_.size(); i++) {
                Column& col = columns_[i];
                col.SetOffset(current_offset);
                current_offset += col.GetFixedLength();

                if (col.GetTypeId() == TypeId::VARCHAR) {
                    tuple_is_inlined_ = false;
                }
            }
            length_ = current_offset;
        }

        // --- Inlined Accessors ---

        inline const std::vector<Column>& GetColumns() const { return columns_; }
        inline const uint32_t GetColumnCount() const { return columns_.size(); }

        inline const Column& GetColumn(uint32_t col_idx) const {
            return columns_[col_idx];
        }

        // Linear scan to find column index by name. O(N).
        inline int32_t GetColIdx(const std::string& col_name) const {
            for (uint32_t i = 0; i < columns_.size(); i++) {
                if (columns_[i].GetName() == col_name) {
                    return static_cast<int32_t>(i);
                }
            }
            return -1;
        }

        // Returns the size of the FIXED HEADER portion of the tuple.
        // This is NOT the total size of the tuple (which varies per row).
        inline uint32_t GetLength() const { return length_; }

        inline uint32_t GetBitmapSize() const { return bitmap_size_; }

        // Returns true if the tuple has NO variable length data (everything is in the header).
        inline bool IsInlined() const { return tuple_is_inlined_; }

        // --- Debug ---

        inline std::string ToString() const {
            std::ostringstream os;
            os << "Schema[";
            for (size_t i = 0; i < columns_.size(); i++) {
                os << columns_[i].ToString();
                if (i < columns_.size() - 1) os << ", ";
            }
            os << "]";
            return os.str();
        }

    private:
        // The actual columns (Modified copy of input)
        std::vector<Column> columns_;

        // Total size of the fixed header
        uint32_t length_{ 0 };

        // Null bitmap size
        uint32_t bitmap_size_{ 0 };

        // Optimization hint: True if we don't need to chase pointers (No VARCHARs)
        bool tuple_is_inlined_{ true };
    };

} // namespace tetodb