// config.h

#pragma once
#include <cstdint>

namespace tetodb {

	// constants
	static constexpr int32_t PAGE_SIZE = 4096;
	static constexpr int32_t INVALID_PAGE_ID = -1;

	// type aliases
	using page_id_t = int32_t;	// identifier for page on disk
	using frame_id_t = int32_t;	// identifier for page in RAM
	using txn_id_t = int32_t;	// Transaction ID Type
	using lsn_t = int32_t;		// Log Sequence Number Type

} // namespace tetodb