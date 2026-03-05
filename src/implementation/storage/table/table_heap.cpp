// table_heap.cpp

#include "storage/table/table_heap.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"
#include "storage/page/page_guard.h"

namespace tetodb {

TableHeap::TableHeap(BufferPoolManager *bpm, LogManager *log_manager,
                     Transaction *txn)
    : bpm_(bpm), log_manager_(log_manager) {
  page_id_t first_page_id;
  Page *page = bpm_->NewPage(&first_page_id);

  WritePageGuard guard(bpm_, page);
  auto table_page = guard.As<TablePage>();

  table_page->Init(first_page_id, PAGE_SIZE);

  guard.MarkDirty();

  first_page_id_ = first_page_id;
  last_page_id_ = first_page_id;
  fsm_.Update(first_page_id, table_page->GetFreeSpaceRemaining());
}

TableHeap::TableHeap(BufferPoolManager *bpm, page_id_t first_page_id,
                     LogManager *log_manager)
    : bpm_(bpm), log_manager_(log_manager), first_page_id_(first_page_id),
      fsm_populated_(false) {
  // Walk pages to find last_page_id_
  page_id_t current_page_id = first_page_id;
  page_id_t prev_page_id = INVALID_PAGE_ID;

  while (current_page_id != INVALID_PAGE_ID) {
    Page *page = bpm_->FetchPage(current_page_id);
    if (page == nullptr) {
      break;
    }

    ReadPageGuard guard(bpm_, page);
    auto table_page = guard.As<TablePage>();

    prev_page_id = current_page_id;
    page_id_t next_page_id = table_page->GetNextPageId();

    if (next_page_id == current_page_id) {
      break; // Self-loop guard
    }

    current_page_id = next_page_id;
  }

  last_page_id_ = prev_page_id;
}

bool TableHeap::InsertTuple(const Tuple &tuple, RID *rid, Transaction *txn,
                            LockManager *lock_mgr) {
  if (tuple.GetSize() + 32 > PAGE_SIZE)
    return false;

  // Lazy FSM population — only scan pages on first INSERT
  if (!fsm_populated_) {
    PopulateFSM();
  }

  page_id_t target_page_id = INVALID_PAGE_ID;
  uint32_t required_space = tuple.GetSize() + SIZE_SLOT;

  {
    std::lock_guard<std::mutex> lock(latch_);
    target_page_id = fsm_.GetBestPage(required_space);
  }

  // FSM confirms no page has space. Allocate a new page.
  if (target_page_id == INVALID_PAGE_ID) {
    page_id_t new_page_id;
    Page *new_page = bpm_->NewPage(&new_page_id);
    if (new_page == nullptr)
      return false;

    WritePageGuard new_guard(bpm_, new_page);
    auto new_table_page = new_guard.As<TablePage>();

    new_table_page->Init(new_page_id, PAGE_SIZE, last_page_id_);

    if (last_page_id_ != INVALID_PAGE_ID) {
      Page *last_page = bpm_->FetchPage(last_page_id_);
      if (last_page != nullptr) {
        WritePageGuard last_guard(bpm_, last_page);
        last_guard.As<TablePage>()->SetNextPageId(new_page_id);
        last_guard.MarkDirty();
      }
    }

    if (!new_table_page->InsertTuple(tuple, rid)) {
      return false;
    }

    // NEW: Atomic Lock Acquisition underneath the guard
    if (txn != nullptr && lock_mgr != nullptr) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        new_table_page->ApplyDelete(*rid);
        new_guard.MarkDirty();
        return false;
      }
    }

    new_guard.MarkDirty();

    {
      std::lock_guard<std::mutex> lock(latch_);
      last_page_id_ = new_page_id;
      fsm_.Update(new_page_id, new_table_page->GetFreeSpaceRemaining());
    }

    if (txn != nullptr) {
      TableWriteRecord rec;
      rec.rid_ = *rid;
      rec.wtype_ = WType::INSERT;
      rec.table_heap_ = this;
      txn->AppendTableWriteRecord(rec);
    }

    if (txn != nullptr && log_manager_ != nullptr) {
      LogRecord page_log(txn->GetTransactionId(), txn->GetPrevLSN(),
                         LogRecordType::NEWPAGE, last_page_id_, new_page_id);
      lsn_t page_lsn = log_manager_->AppendLogRecord(&page_log);
      txn->SetPrevLSN(page_lsn);
      if (last_page_id_ != INVALID_PAGE_ID) {
        Page *last_page = bpm_->FetchPage(last_page_id_);
        if (last_page) {
          WritePageGuard last_guard(bpm_, last_page);
          last_guard.As<TablePage>()->SetLSN(page_lsn);
        }
      }

      LogRecord insert_log(txn->GetTransactionId(), txn->GetPrevLSN(),
                           LogRecordType::INSERT, *rid, tuple);
      lsn_t insert_lsn = log_manager_->AppendLogRecord(&insert_log);
      txn->SetPrevLSN(insert_lsn);
      new_table_page->SetLSN(insert_lsn);
    }
    return true;
  }

  // FSM found a valid page. Use it.
  Page *page = bpm_->FetchPage(target_page_id);
  if (page == nullptr)
    return false;

  WritePageGuard guard(bpm_, page);
  auto table_page = guard.As<TablePage>();

  if (table_page->InsertTuple(tuple, rid)) {
    // NEW: Atomic Lock Acquisition underneath the guard
    if (txn != nullptr && lock_mgr != nullptr) {
      if (!lock_mgr->LockExclusive(txn, *rid)) {
        table_page->ApplyDelete(*rid);
        guard.MarkDirty();
        return false;
      }
    }

    guard.MarkDirty();

    {
      std::lock_guard<std::mutex> lock(latch_);
      fsm_.Update(target_page_id, table_page->GetFreeSpaceRemaining());
    }

    if (txn != nullptr) {
      TableWriteRecord rec;
      rec.rid_ = *rid;
      rec.wtype_ = WType::INSERT;
      rec.table_heap_ = this;
      txn->AppendTableWriteRecord(rec);
    }

    if (txn != nullptr && log_manager_ != nullptr) {
      LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                           LogRecordType::INSERT, *rid, tuple);
      lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
      txn->SetPrevLSN(lsn);
      table_page->SetLSN(lsn);
    }

    return true;
  }

  return false;
}

bool TableHeap::GetTuple(const RID &rid, Tuple *tuple, Transaction *txn) {
  Page *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr)
    return false;

  ReadPageGuard guard(bpm_, page);
  return guard.As<TablePage>()->GetTuple(rid, tuple);
}

bool TableHeap::MarkDelete(const RID &rid, Transaction *txn) {
  Page *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr)
    return false;

  WritePageGuard guard(bpm_, page);

  Tuple deleted_tuple;
  bool has_tuple = guard.As<TablePage>()->GetTuple(rid, &deleted_tuple);

  if (guard.As<TablePage>()->MarkDelete(rid)) {
    guard.MarkDirty();

    {
      std::lock_guard<std::mutex> lock(latch_);
      fsm_.Update(rid.GetPageId(),
                  guard.As<TablePage>()->GetFreeSpaceRemaining());
    }

    if (txn != nullptr) {
      TableWriteRecord rec;
      rec.rid_ = rid;
      rec.wtype_ = WType::DELETE;
      rec.tuple_ = deleted_tuple;
      rec.table_heap_ = this;
      txn->AppendTableWriteRecord(rec);
    }

    if (txn != nullptr && log_manager_ != nullptr && has_tuple) {
      LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                           LogRecordType::MARKDELETE, rid, deleted_tuple);
      lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
      txn->SetPrevLSN(lsn);
      guard.As<TablePage>()->SetLSN(lsn);
    }

    return true;
  }

  return false;
}

bool TableHeap::UpdateTuple(const Tuple &tuple, RID *rid, Transaction *txn,
                            LockManager *lock_mgr) {
  Page *page = bpm_->FetchPage(rid->GetPageId());
  if (page == nullptr)
    return false;

  {
    WritePageGuard guard(bpm_, page);

    Tuple old_tuple;
    guard.As<TablePage>()->GetTuple(*rid, &old_tuple);

    if (!guard.As<TablePage>()->MarkDelete(*rid)) {
      return false;
    }
    guard.MarkDirty();

    {
      std::lock_guard<std::mutex> lock(latch_);
      fsm_.Update(rid->GetPageId(),
                  guard.As<TablePage>()->GetFreeSpaceRemaining());
    }

    if (txn != nullptr) {
      TableWriteRecord rec;
      rec.rid_ = *rid;
      rec.wtype_ = WType::DELETE;
      rec.tuple_ = old_tuple;
      rec.table_heap_ = this;
      txn->AppendTableWriteRecord(rec);
    }

    if (txn != nullptr && log_manager_ != nullptr) {
      LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                           LogRecordType::MARKDELETE, *rid, old_tuple);
      lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
      txn->SetPrevLSN(lsn);
      guard.As<TablePage>()->SetLSN(lsn);
    }
  }

  RID new_rid;
  if (InsertTuple(tuple, &new_rid, txn, lock_mgr)) {
    *rid = new_rid;
    return true;
  }

  return false;
}

bool TableHeap::RollbackDelete(const RID &rid, const Tuple &tuple,
                               Transaction *txn) {
  Page *page = bpm_->FetchPage(rid.GetPageId());
  if (page == nullptr)
    return false;

  WritePageGuard guard(bpm_, page);
  auto table_page = guard.As<TablePage>();

  if (table_page->RollbackDelete(rid, tuple)) {
    guard.MarkDirty();

    {
      std::lock_guard<std::mutex> lock(latch_);
      fsm_.Update(rid.GetPageId(), table_page->GetFreeSpaceRemaining());
    }

    if (txn != nullptr && log_manager_ != nullptr) {
      LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(),
                           LogRecordType::ROLLBACKDELETE, rid, tuple);
      lsn_t lsn = log_manager_->AppendLogRecord(&log_record);
      txn->SetPrevLSN(lsn);
      table_page->SetLSN(lsn);
    }

    return true;
  }

  return false;
}

void TableHeap::Destroy() {
  page_id_t current_page_id = first_page_id_;

  while (current_page_id != INVALID_PAGE_ID) {
    page_id_t next_page_id;

    {
      Page *page = bpm_->FetchPage(current_page_id);
      if (page == nullptr) {
        break;
      }

      ReadPageGuard guard(bpm_, page);
      next_page_id = guard.As<TablePage>()->GetNextPageId();
    }

    bpm_->DeletePage(current_page_id);

    if (current_page_id == next_page_id)
      break;

    current_page_id = next_page_id;
  }

  first_page_id_ = INVALID_PAGE_ID;
  last_page_id_ = INVALID_PAGE_ID;

  std::lock_guard<std::mutex> lock(latch_);
  fsm_.Clear();
}

void TableHeap::PopulateFSM() {
  std::lock_guard<std::mutex> lock(latch_);
  if (fsm_populated_)
    return; // Double-check under lock

  page_id_t current_page_id = first_page_id_;
  while (current_page_id != INVALID_PAGE_ID) {
    Page *page = bpm_->FetchPage(current_page_id);
    if (page == nullptr)
      break;

    ReadPageGuard guard(bpm_, page);
    auto table_page = guard.As<TablePage>();

    fsm_.Update(current_page_id, table_page->GetFreeSpaceRemaining());

    page_id_t next_page_id = table_page->GetNextPageId();
    if (next_page_id == current_page_id)
      break;
    current_page_id = next_page_id;
  }

  fsm_populated_ = true;
}

} // namespace tetodb