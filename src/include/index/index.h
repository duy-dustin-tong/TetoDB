// index.h

#pragma once

#include <vector>
#include <string>
#include "storage/table/tuple.h"
#include "common/record_id.h"
#include "concurrency/transaction.h"

namespace tetodb {

    class Index {
    public:
        virtual ~Index() = default;

        virtual void InsertEntry(const Tuple& key_tuple, RID rid, Transaction* txn) = 0;

        // Note: RID is explicitly required here!
        virtual void DeleteEntry(const Tuple& key_tuple, RID rid, Transaction* txn) = 0;

        virtual void ScanKey(const Tuple& key_tuple, std::vector<RID>* result, Transaction* txn) = 0;

        virtual void Destroy() = 0;

        // Exposing metadata to the Catalog
        virtual std::string GetName() const = 0;
        virtual const Schema* GetKeySchema() const = 0;
        virtual page_id_t GetRootPageId() const = 0; // <-- NEW
    };

} // namespace tetodb