// tetodb_instance.h

#pragma once

#include <memory>
#include <mutex>
#include <string>


#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"
#include "recovery/checkpoint_manager.h"
#include "recovery/log_manager.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "storage/buffer/two_queue_replacer.h"
#include "storage/disk/disk_manager.h"


namespace tetodb {

// Represents a single client's connection state
struct ClientSession {
  Transaction *active_txn = nullptr;
  bool is_poisoned = false;
  std::unordered_map<std::string, std::string> prepared_statements;
  std::vector<Value> current_parameters;
};

struct QueryResult {
  std::string status_msg;         // e.g., "INSERT 0 1" or "CREATE TABLE"
  std::vector<Tuple> rows;        // Raw data rows
  const Schema *schema = nullptr; // Schema for column names/types
  std::shared_ptr<Schema>
      owned_schema; // Owns the schema if dynamically generated
  bool is_error = false;
};

class TetoDBInstance {
public:
  // Boots the database, runs ARIES recovery, and starts background threads
  TetoDBInstance(const std::string &db_file_name);

  // Flushes buffers, stops threads, and cleanly shuts down
  ~TetoDBInstance();

  QueryResult ExecuteQuery(const std::string &sql, ClientSession &session);

private:
  std::unique_ptr<DiskManager> disk_manager_;
  std::unique_ptr<TwoQueueReplacer> replacer_;
  std::unique_ptr<BufferPoolManager> bpm_;
  std::unique_ptr<LogManager> log_mgr_;
  std::unique_ptr<LockManager> lock_mgr_;
  std::unique_ptr<TransactionManager> txn_mgr_;
  std::unique_ptr<Catalog> catalog_;
  std::unique_ptr<CheckpointManager> checkpoint_mgr_;

  std::mutex execution_mutex_; // Prevents concurrent DDL/DML conflicts
};

} // namespace tetodb
