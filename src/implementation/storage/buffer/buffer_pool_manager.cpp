// buffer_pool_manager.cpp

#include "storage/buffer/buffer_pool_manager.h"

namespace tetodb {
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                     DiskManager *disk_manager,
                                     TwoQueueReplacer *replacer)
    : pool_size_(pool_size), disk_manager_(disk_manager), replacer_(replacer) {
  size_t bytes = pool_size_ * PAGE_SIZE;
  char *raw_ptr = nullptr;

#ifdef _WIN32
  raw_ptr = static_cast<char *>(_aligned_malloc(bytes, PAGE_SIZE));
  if (!raw_ptr)
    throw std::bad_alloc();
#else
  if (posix_memalign((void **)&raw_ptr, PAGE_SIZE, bytes) != 0) {
    throw std::bad_alloc();
  }
#endif

  data_pool_.reset(raw_ptr);
  pages_.resize(pool_size_);

  for (size_t i = 0; i < pool_size_; i++) {
    pages_[i].data_ = data_pool_.get() + (i * PAGE_SIZE);
    free_list_.push_back(static_cast<frame_id_t>(i));
  }
}

BufferPoolManager::~BufferPoolManager() = default;

bool BufferPoolManager::GetFreeFrame(frame_id_t *frame_id) {
  if (!free_list_.empty()) {
    *frame_id = free_list_.front();
    free_list_.pop_front();
    return true;
  }

  if (replacer_->Evict(frame_id)) {
    Page *evicted_page = &pages_[*frame_id];

    if (evicted_page->is_dirty_) {
      if (!disk_manager_->WritePage(evicted_page->GetPageId(),
                                    evicted_page->GetData())) {
        replacer_->SetEvictable(*frame_id, true);
        return false;
      }
      evicted_page->is_dirty_ = false;
    }

    page_table_.erase(evicted_page->GetPageId());
    return true;
  }

  return false;
}

Page *BufferPoolManager::NewPage(page_id_t *page_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id = INVALID_FRAME_ID;

  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }

  *page_id = disk_manager_->AllocatePage();

  Page *page = &pages_[frame_id];

  page->page_id_ = *page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = true;
  page->ResetMemory();

  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  // FIX: Removed '&' to prevent dangling reference
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    page->pin_count_++;

    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);

    return page;
  }

  frame_id_t frame_id = INVALID_FRAME_ID;

  if (!GetFreeFrame(&frame_id)) {
    return nullptr;
  }

  Page *page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, page->GetData());

  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock(latch_);

  // FIX: Removed '&' to prevent dangling reference
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ <= 0)
    return false;

  page->pin_count_--;
  page->is_dirty_ |= is_dirty;

  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  // FIX: Removed '&' to prevent dangling reference
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;
  }

  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->ResetMemory();

  page_table_.erase(page_id);
  replacer_->Remove(frame_id);

  free_list_.push_back(frame_id);

  disk_manager_->DeallocatePage(page_id);
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  // FIX: Removed '&' to prevent dangling reference
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (!disk_manager_->WritePage(page_id, page->GetData())) {
    return false;
  }
  page->is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    Page *page = &pages_[i];

    if (page->page_id_ != INVALID_PAGE_ID && page->is_dirty_) {
      if (disk_manager_->WritePage(page->page_id_, page->GetData())) {
        page->is_dirty_ = false;
      }
    }
  }
}

} // namespace tetodb
