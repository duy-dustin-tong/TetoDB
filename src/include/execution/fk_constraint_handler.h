// fk_constraint_handler.h

#pragma once

#include "catalog/catalog.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"
#include <functional>

namespace tetodb {

// Shared FK constraint enforcement logic, used by DELETE and UPDATE executors.
class FKConstraintHandler {
public:
  using WriteLockFn = std::function<bool(const RID &)>;

  // Enforce ON DELETE constraints for all child tables referencing
  // parent_table. Handles RESTRICT (throws), CASCADE (deletes children),
  // SET_NULL (nullifies FK column).
  static void EnforceOnDelete(const Tuple &deleted_tuple,
                              TableMetadata *parent_table, Catalog *catalog,
                              Transaction *txn,
                              const WriteLockFn &acquire_write_lock);
};

} // namespace tetodb
