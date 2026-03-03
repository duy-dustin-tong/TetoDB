// index_write_record.h

#pragma once

#include "common/record_id.h"
#include "storage/table/tuple.h"
#include "concurrency/table_write_record.h" // For WType enum

namespace tetodb {

    class Index;

    class IndexWriteRecord {
    public:
        IndexWriteRecord() = default;

        IndexWriteRecord(RID rid, WType wtype, const Tuple& tuple, Index* index)
            : rid_(rid), wtype_(wtype), tuple_(tuple), index_(index) {
        }

        RID rid_;
        WType wtype_;
        Tuple tuple_; // The actual index key tuple
        Index* index_;
    };

} // namespace tetodb