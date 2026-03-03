// recovery_manager.h

#pragma once

#include <unordered_map>
#include <fstream>
#include <filesystem>
#include <string>

#include "recovery/log_record.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager.h"
#include "storage/page/table_page.h"

namespace tetodb {

    class RecoveryManager {
    public:
        RecoveryManager(DiskManager* disk_manager, BufferPoolManager* bpm, const std::string& db_file_name)
            : disk_manager_(disk_manager), bpm_(bpm) {

            // Reconstruct the log file name from the DB name
            std::filesystem::path path(db_file_name);
            path.replace_extension(".log");
            log_file_name_ = path.string();
        }

        ~RecoveryManager() = default;

        /**
         * Phase 1 & 2: Analyzes the log and replays it from start to finish.
         * Restores the exact physical state of the database at the moment of the crash.
         */
        void Redo();

        /**
         * Phase 3: Reverts the changes made by transactions that never committed.
         * Uses the Active Transaction Table built during Redo.
         */
        void Undo();

    private:
        DiskManager* disk_manager_;
        BufferPoolManager* bpm_;
        std::string log_file_name_;

        // --- ACTIVE TRANSACTION TABLE (ATT) ---
        // Tracks transactions that were running when the crash happened.
        // Maps txn_id -> the LSN of their most recent log record.
        std::unordered_map<txn_id_t, lsn_t> active_txn_;

        // --- LSN TO OFFSET MAPPING ---
        // Maps an LSN to its physical byte offset in the log file.
        // This allows us to instantly jump backwards through the file during the Undo phase.
        std::unordered_map<lsn_t, uint32_t> lsn_mapping_;
    };

} // namespace tetodb