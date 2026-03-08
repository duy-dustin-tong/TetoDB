// log_record.h

#pragma once

#include "common/config.h" // Assuming txn_id_t is here
#include "common/record_id.h"
#include "storage/table/tuple.h"
#include <string>


namespace tetodb {

// Unique ID for every log record ever created
using lsn_t = int32_t;

/**
 * The types of actions we need to record to ensure durability.
 */
enum class LogRecordType {
  INVALID = 0,
  INSERT,
  MARKDELETE,
  APPLYDELETE,
  ROLLBACKDELETE,
  UPDATE,
  BEGIN,
  COMMIT,
  ABORT,
  NEWPAGE,
  CHECKPOINT // <-- NEW!
};

/**
 * LogRecord represents a single physical change in the database.
 * It contains everything needed to Redo (repeat) or Undo (rollback) an action.
 */
class LogRecord {
public:
  // Default constructor
  LogRecord() = default;

  // --- Constructor 1: Transaction Control (BEGIN, COMMIT, ABORT) ---
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type)
      : txn_id_(txn_id), prev_lsn_(prev_lsn),
        log_record_type_(log_record_type) {}

  // --- Constructor 2: INSERT / DELETE ---
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            const RID &rid, const Tuple &tuple)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(log_record_type),
        target_rid_(rid), new_tuple_(tuple) {}

  // --- Constructor 3: UPDATE (Needs both Old and New Tuples for Undo/Redo) ---
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            const RID &rid, const Tuple &old_tuple, const Tuple &new_tuple)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(log_record_type),
        target_rid_(rid), old_tuple_(old_tuple), new_tuple_(new_tuple) {}

  // --- Constructor 4: NEWPAGE ---
  LogRecord(txn_id_t txn_id, lsn_t prev_lsn, LogRecordType log_record_type,
            page_id_t prev_page_id, page_id_t page_id)
      : txn_id_(txn_id), prev_lsn_(prev_lsn), log_record_type_(log_record_type),
        prev_page_id_(prev_page_id) {
    target_rid_ = RID(page_id, 0); // Re-use RID to store the new page ID
  }

  // --- Getters ---
  inline lsn_t GetLSN() const { return lsn_; }
  inline void SetLSN(lsn_t lsn) { lsn_ = lsn; }
  inline lsn_t GetPrevLSN() const { return prev_lsn_; }
  inline txn_id_t GetTxnId() const { return txn_id_; }
  inline LogRecordType GetLogRecordType() const { return log_record_type_; }

  inline bool IsCLR() const { return is_clr_ != 0; }
  inline void SetCLR(bool is_clr) { is_clr_ = is_clr ? 1 : 0; }
  inline lsn_t GetUndoNextLSN() const { return undo_next_lsn_; }
  inline void SetUndoNextLSN(lsn_t lsn) { undo_next_lsn_ = lsn; }

  // --- Payload Getters ---
  inline RID GetTargetRID() const { return target_rid_; }
  inline Tuple GetOldTuple() const { return old_tuple_; }
  inline Tuple GetNewTuple() const { return new_tuple_; }

  // We will need size to know how many bytes to write to the disk!
  inline uint32_t GetSize() const { return size_; }
  inline void SetSize(uint32_t size) { size_ = size; }

  inline page_id_t GetPrevPageId() const { return prev_page_id_; }

  /**
   * Flattens this LogRecord into a raw byte array.
   * @return The total number of bytes written.
   */
  uint32_t Serialize(char *dest) const;

  /**
   * Reconstructs a LogRecord from a raw byte array.
   * @return The total number of bytes read.
   */
  uint32_t Deserialize(const char *src);

  // Helper to calculate the exact size this record will take on disk
  static uint32_t CalculateSize(LogRecordType type, const Tuple &old_tuple,
                                const Tuple &new_tuple);

private:
  // ==========================================
  // HEADER (Always written)
  // ==========================================
  uint32_t size_{0};            // Total physical size of this record in bytes
  lsn_t lsn_{INVALID_LSN};      // Log Sequence Number (The ID of this record)
  txn_id_t txn_id_{0};          // The Transaction that caused this
  lsn_t prev_lsn_{INVALID_LSN}; // The previous LSN for this specific
                                // transaction (Linked List!)
  LogRecordType log_record_type_{LogRecordType::INVALID};
  uint32_t is_clr_{0}; // NEW: 1 if this is a Compensation Log Record
  lsn_t undo_next_lsn_{
      INVALID_LSN}; // NEW: LSN of the next record to undo (for CLRs)

  // ==========================================
  // PAYLOAD (Written based on LogRecordType)
  // ==========================================
  RID target_rid_;  // The physical location modified
  Tuple old_tuple_; // The data BEFORE the change (Used for UNDO)
  Tuple new_tuple_; // The data AFTER the change (Used for REDO)

  page_id_t prev_page_id_{INVALID_PAGE_ID};
};

} // namespace tetodb