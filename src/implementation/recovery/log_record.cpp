// log_record.cpp

#include "recovery/log_record.h"
#include <cstring>

namespace tetodb {

    // ==========================================
    // SIZE CALCULATION
    // ==========================================
    uint32_t LogRecord::CalculateSize(LogRecordType type, const Tuple& old_tuple, const Tuple& new_tuple) {
        // Standard Header Size: size (4) + lsn (4) + txn_id (4) + prev_lsn (4) + type (4) = 20 bytes
        uint32_t size = 20;

        switch (type) {
        case LogRecordType::INSERT:
        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE:
            size += sizeof(RID);
            size += sizeof(uint32_t); // NEW: Space to store the tuple's size!
            size += new_tuple.GetSize(); // Updated to GetSize()
            break;
        case LogRecordType::UPDATE:
            size += sizeof(RID);
            size += sizeof(uint32_t); // Space for old_tuple size
            size += old_tuple.GetSize();
            size += sizeof(uint32_t); // Space for new_tuple size
            size += new_tuple.GetSize();
            break;
        case LogRecordType::NEWPAGE:
            size += sizeof(RID); // Stores new_page_id
            size += sizeof(page_id_t); // Stores prev_page_id
            break;

        case LogRecordType::BEGIN:
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
            // No payload! Just the 20-byte header.
            break;
        
        default:
            break;
        }
        return size;
    }

    // ==========================================
    // SERIALIZATION (C++ Object -> Raw Bytes)
    // ==========================================
    uint32_t LogRecord::Serialize(char* dest) const {
        uint32_t offset = 0;

        // 1. Write Header
        std::memcpy(dest + offset, &size_, sizeof(uint32_t)); offset += sizeof(uint32_t);
        std::memcpy(dest + offset, &lsn_, sizeof(lsn_t)); offset += sizeof(lsn_t);
        std::memcpy(dest + offset, &txn_id_, sizeof(txn_id_t)); offset += sizeof(txn_id_t);
        std::memcpy(dest + offset, &prev_lsn_, sizeof(lsn_t)); offset += sizeof(lsn_t);
        std::memcpy(dest + offset, &log_record_type_, sizeof(LogRecordType)); offset += sizeof(LogRecordType);

        // 2. Write Payload
        if (log_record_type_ == LogRecordType::INSERT ||
            log_record_type_ == LogRecordType::MARKDELETE ||
            log_record_type_ == LogRecordType::APPLYDELETE ||
            log_record_type_ == LogRecordType::ROLLBACKDELETE) {

            std::memcpy(dest + offset, &target_rid_, sizeof(RID)); offset += sizeof(RID);

            // WRITE THE SIZE PREFIX FIRST
            uint32_t tup_size = new_tuple_.GetSize();
            std::memcpy(dest + offset, &tup_size, sizeof(uint32_t)); offset += sizeof(uint32_t);

            // WRITE THE RAW TUPLE BYTES
            new_tuple_.SerializeTo(dest + offset);
            offset += tup_size;
        }
        else if (log_record_type_ == LogRecordType::UPDATE) {
            std::memcpy(dest + offset, &target_rid_, sizeof(RID)); offset += sizeof(RID);

            // OLD TUPLE (Size + Bytes)
            uint32_t old_size = old_tuple_.GetSize();
            std::memcpy(dest + offset, &old_size, sizeof(uint32_t)); offset += sizeof(uint32_t);
            old_tuple_.SerializeTo(dest + offset);
            offset += old_size;

            // NEW TUPLE (Size + Bytes)
            uint32_t new_size = new_tuple_.GetSize();
            std::memcpy(dest + offset, &new_size, sizeof(uint32_t)); offset += sizeof(uint32_t);
            new_tuple_.SerializeTo(dest + offset);
            offset += new_size;
        }
        else if (log_record_type_ == LogRecordType::NEWPAGE) {
            std::memcpy(dest + offset, &target_rid_, sizeof(RID)); offset += sizeof(RID);
            std::memcpy(dest + offset, &prev_page_id_, sizeof(page_id_t)); offset += sizeof(page_id_t);
        }

        return offset;
    }

    // ==========================================
    // DESERIALIZATION (Raw Bytes -> C++ Object)
    // ==========================================
    uint32_t LogRecord::Deserialize(const char* src) {
        uint32_t offset = 0;

        // 1. Read Header
        std::memcpy(&size_, src + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);
        std::memcpy(&lsn_, src + offset, sizeof(lsn_t)); offset += sizeof(lsn_t);
        std::memcpy(&txn_id_, src + offset, sizeof(txn_id_t)); offset += sizeof(txn_id_t);
        std::memcpy(&prev_lsn_, src + offset, sizeof(lsn_t)); offset += sizeof(lsn_t);
        std::memcpy(&log_record_type_, src + offset, sizeof(LogRecordType)); offset += sizeof(LogRecordType);

        // 2. Read Payload
        if (log_record_type_ == LogRecordType::INSERT ||
            log_record_type_ == LogRecordType::MARKDELETE ||
            log_record_type_ == LogRecordType::APPLYDELETE ||
            log_record_type_ == LogRecordType::ROLLBACKDELETE) {

            std::memcpy(&target_rid_, src + offset, sizeof(RID)); offset += sizeof(RID);

            // READ THE SIZE PREFIX FIRST
            uint32_t tup_size = 0;
            std::memcpy(&tup_size, src + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);

            // PASS SIZE TO YOUR TUPLE API
            new_tuple_.DeserializeFrom(src + offset, tup_size);
            offset += tup_size;
        }
        else if (log_record_type_ == LogRecordType::UPDATE) {
            std::memcpy(&target_rid_, src + offset, sizeof(RID)); offset += sizeof(RID);

            // OLD TUPLE
            uint32_t old_size = 0;
            std::memcpy(&old_size, src + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);
            old_tuple_.DeserializeFrom(src + offset, old_size);
            offset += old_size;

            // NEW TUPLE
            uint32_t new_size = 0;
            std::memcpy(&new_size, src + offset, sizeof(uint32_t)); offset += sizeof(uint32_t);
            new_tuple_.DeserializeFrom(src + offset, new_size);
            offset += new_size;
        }
        else if (log_record_type_ == LogRecordType::NEWPAGE) {
            std::memcpy(&target_rid_, src + offset, sizeof(RID)); offset += sizeof(RID);
            std::memcpy(&prev_page_id_, src + offset, sizeof(page_id_t)); offset += sizeof(page_id_t);
        }

        return offset;
    }

} // namespace tetodb