// page.h

// Role: A wrapper that points a 4KB block in buffer.
// Exposes: GetData(), GetPageId(), RLatch(), WLatch().

#pragma once

#include "common/config.h"
#include "common/rwlatch.h"
#include <algorithm>
#include <array>
#include <cstring>
#include <iostream>


namespace tetodb {

class Page {
  friend class BufferPoolManager;

public:
  Page() = default;

  Page(const Page &) = delete;
  Page &operator=(const Page &) = delete;

  Page(Page &&other) noexcept
      : page_id_(other.page_id_), pin_count_(other.pin_count_),
        is_dirty_(other.is_dirty_), data_(other.data_) {
    other.page_id_ = INVALID_PAGE_ID;
    other.data_ = nullptr;
  }

  Page &operator=(Page &&other) noexcept {
    if (this != &other) {
      page_id_ = other.page_id_;
      pin_count_ = other.pin_count_;
      is_dirty_ = other.is_dirty_;
      data_ = other.data_;

      other.page_id_ = INVALID_PAGE_ID;
      other.data_ = nullptr;
    }
    return *this;
  }

  ~Page() = default;

  inline char *GetData() { return data_; }
  inline page_id_t GetPageId() const { return page_id_; }
  inline int32_t GetPinCount() { return pin_count_; }
  inline void ResetMemory() {
    if (data_)
      memset(data_, 0, PAGE_SIZE);
  }

  inline void WLatch() { rwlatch_.WLock(); }
  inline void WUnlatch() { rwlatch_.WUnlock(); }
  inline void RLatch() { rwlatch_.RLock(); }
  inline void RUnlatch() { rwlatch_.RUnlock(); }

protected:
  // meta data
  page_id_t page_id_{INVALID_PAGE_ID};
  int32_t pin_count_{0};
  bool is_dirty_{0};

  // RWLatch
  ReaderWriterLatch rwlatch_;

  // actual data address
  char *data_{
      nullptr}; // use raw pointer here because memory is allocated in buffer
};

} // namespace tetodb