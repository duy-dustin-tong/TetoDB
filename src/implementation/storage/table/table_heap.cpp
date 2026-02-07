// table_heap.cpp


#include "storage/table/table_heap.h"
#include "storage/page/page_guard.h"

namespace tetodb {

    // CONSTRUCTOR 1: New Table
    TableHeap::TableHeap(BufferPoolManager* bpm, Transaction* txn) : bpm_(bpm) {
        // 1. Allocate First Page
        page_id_t first_page_id;
        Page* page = bpm_->NewPage(&first_page_id);

        // 2. Guard it immediately (Auto-Unpin on exit)
        // Note: NewPage returns a pinned page. The Guard constructor usually expects to just Latch.
        // However, since we just allocated it, we are the only owner. 
        // We can just cast and init.

        // RAII approach: Wrap it in a WritePageGuard to ensure it gets unpinned/unlatched correctly.
        WritePageGuard guard(bpm_, page);
        auto table_page = guard.As<TablePage>();

        table_page->Init(first_page_id, PAGE_SIZE);
        guard.MarkDirty(); // Ensure it gets written to disk

        first_page_id_ = first_page_id;
    }

    // CONSTRUCTOR 2: Existing Table
    TableHeap::TableHeap(BufferPoolManager* bpm, page_id_t first_page_id)
        : bpm_(bpm), first_page_id_(first_page_id) {
    }


    // INSERT OPERATION
    bool TableHeap::InsertTuple(const Tuple& tuple, RID* rid, Transaction* txn) {
        if (tuple.GetSize() + 32 > PAGE_SIZE) return false;

        // 1. Optimistic: Try the cached Last Page directly
        Page* page = bpm_->FetchPage(last_page_id_);
        if (page == nullptr) return false;

        WritePageGuard guard(bpm_, page);
        auto table_page = guard.As<TablePage>();

        if (table_page->InsertTuple(tuple, rid)) {
            guard.MarkDirty();
            return true;
        }

        // 2. Failure: Last page is full. We need a new page.
        // (We don't scan previous pages for holes in this design - we just append)

        page_id_t new_page_id;
        Page* new_raw_page = bpm_->NewPage(&new_page_id);
        if (new_raw_page == nullptr) return false;

        // Link Old -> New
        table_page->SetNextPageId(new_page_id);
        guard.MarkDirty();

        // Grab the old page ID before we drop the guard
        page_id_t old_page_id = table_page->GetPageId();
        guard.Drop(); // Release lock on old last page

        // 3. Setup New Page
        guard = WritePageGuard(bpm_, new_raw_page);
        auto new_table_page = guard.As<TablePage>();

        new_table_page->Init(new_page_id, PAGE_SIZE, old_page_id);

        // 4. Update the Cache
        last_page_id_ = new_page_id; // <--- Critical update

        // 5. Insert into the new page (Guaranteed to succeed since it's empty)
        new_table_page->InsertTuple(tuple, rid);
        guard.MarkDirty();

        return true;
    }


    // GET OPERATION
    bool TableHeap::GetTuple(const RID& rid, Tuple* tuple, Transaction* txn) {
        Page* page = bpm_->FetchPage(rid.GetPageId());
        if (page == nullptr) return false;

        // RAII: Automatically RLatch() on construction, RUnlatch()+Unpin() on destruction
        ReadPageGuard guard(bpm_, page);

        return guard.As<TablePage>()->GetTuple(rid, tuple);
    }


    // DELETE OPERATION
    bool TableHeap::MarkDelete(const RID& rid, Transaction* txn) {
        // TO DO: Implement a free_list 
        // Currently marks slot as deleted and ignore forever



        Page* page = bpm_->FetchPage(rid.GetPageId());
        if (page == nullptr) return false;

        // RAII: Automatically WLatch()
        WritePageGuard guard(bpm_, page);

        if (guard.As<TablePage>()->MarkDelete(rid)) {
            guard.MarkDirty(); 
            return true;
        }

        return false;
    }


    // UPDATE OPERATION
    bool TableHeap::UpdateTuple(const Tuple& tuple, RID* rid, Transaction* txn) {
        // 1. Find the old page
        Page* page = bpm_->FetchPage(rid->GetPageId());
        if (page == nullptr) return false;

        // 2. Delete the old tuple
        // Scope the guard so it releases the lock immediately after delete
        {
            WritePageGuard guard(bpm_, page);
            if (!guard.As<TablePage>()->MarkDelete(*rid)) {
                return false;
            }
            guard.MarkDirty();
        } // Guard destroys here -> Page Unlatched & Unpinned

        // 3. Insert the new tuple
        // This starts a fresh search from the beginning of the heap
        RID new_rid;
        if (InsertTuple(tuple, &new_rid, txn)) {
            *rid = new_rid;
            return true;
        }

        // If insert fails (out of space?), technically we should UNDO the delete.
        // But without transactions/logging, we can't easily undo yet.
        return false;
    }

} // namespace tetodb