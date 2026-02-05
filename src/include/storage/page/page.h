// page.h

// Role: A wrapper that points a 4KB block in buffer.
// Exposes: GetData(), GetPageId(), RLatch(), WLatch().

#pragma once

#include <array>
#include <algorithm>
#include <iostream>
#include "common/config.h"

namespace tetodb {

	class Page {
		friend class BufferPoolManager;

	public:
		Page() = default;
		~Page() = default;

		inline char* GetData() { return data_; }
		inline page_id_t GetPageId() const { return page_id_; }
		//inline int32_t GetPinCount() { return pin_count_; }
		inline void ResetMemory() {
			if (data_) memset(data_, 0, PAGE_SIZE);
		}

	protected:
		// meta data
		page_id_t page_id_{ INVALID_PAGE_ID };
		//int32_t pin_count_{ 0 };
		bool is_dirty_{ 0 };

		// actual data address
		char* data_; // use raw pointer here because memory is allocated in buffer

	};

} // namespace tetodb