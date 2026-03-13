// execution_context.h

#pragma once
#include <vector>
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "concurrency/transaction_manager.h" 
#include "type/value.h"

namespace tetodb {

    class ExecutionContext {
    public:
        ExecutionContext(Catalog* catalog, BufferPoolManager* bpm, Transaction* txn,
            LockManager* lock_mgr, TransactionManager* txn_mgr,
            const std::vector<Value>* params = nullptr) // <-- NEW
            : catalog_(catalog), bpm_(bpm), txn_(txn), lock_mgr_(lock_mgr), txn_mgr_(txn_mgr), params_(params) {
        }

        inline Catalog* GetCatalog() { return catalog_; }
        inline BufferPoolManager* GetBufferPoolManager() { return bpm_; }
        inline Transaction* GetTransaction() { return txn_; }
        inline LockManager* GetLockManager() { return lock_mgr_; }
        inline TransactionManager* GetTransactionManager() { return txn_mgr_; }

        inline const std::vector<Value>* GetParams() const { return params_; } // <-- NEW

    private:
        Catalog* catalog_;
        BufferPoolManager* bpm_;
        Transaction* txn_;
        LockManager* lock_mgr_;
        TransactionManager* txn_mgr_;
        const std::vector<Value>* params_;
    };

} // namespace tetodb