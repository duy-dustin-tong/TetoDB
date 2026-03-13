// rwlatch.h

#pragma once

#include <shared_mutex>
#include <mutex>

namespace tetodb {
	class ReaderWriterLatch {
	public:
		inline void WLock() { mutex_.lock(); }
		inline void WUnlock() { mutex_.unlock(); }
		inline void RLock() { mutex_.lock_shared(); }
		inline void RUnlock() { mutex_.unlock_shared(); }


	private:
		std::shared_mutex mutex_;
	};

} // namespace tetodb