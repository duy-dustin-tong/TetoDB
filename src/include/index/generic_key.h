#pragma once

#include "type/type_id.h"
#include "type/value.h"
#include <algorithm>
#include <cstring>

namespace tetodb {

/**
 * GenericKey is a fixed-size container for indexing.
 * It flattens a Value object into a raw byte array for the B+ Tree.
 */
template <size_t KeySize> class GenericKey {
public:
  // The actual data storage (Fixed size on the page)
  char data_[KeySize];

  inline GenericKey() { memset(data_, 0, KeySize); }

  // -----------------------------------------------------------------------
  // CONVERT VALUE -> GENERIC KEY
  // This extracts the raw bytes from your Value class based on its TypeId.
  // -----------------------------------------------------------------------
  inline void SetFromValue(const Value &val) {
    memset(data_, 0, KeySize);

    switch (val.GetTypeId()) {
    case TypeId::BOOLEAN: {
      bool v = val.GetAsBoolean();
      memcpy(data_, &v, 1);
      break;
    }
    case TypeId::TINYINT: {
      // Your Value class stores everything in the union.
      // We cast down to ensure we only grab the relevant byte.
      int8_t v = static_cast<int8_t>(val.GetAsInteger());
      memcpy(data_, &v, 1);
      break;
    }
    case TypeId::SMALLINT: {
      int16_t v = static_cast<int16_t>(val.GetAsInteger());
      memcpy(data_, &v, 2);
      break;
    }
    case TypeId::INTEGER: {
      int32_t v = val.GetAsInteger();
      memcpy(data_, &v, 4);
      break;
    }
    case TypeId::BIGINT: {
      int64_t v = val.GetAsBigInt();
      memcpy(data_, &v, 8);
      break;
    }
    case TypeId::DECIMAL: {
      double v = val.GetAsDecimal();
      memcpy(data_, &v, 8);
      break;
    }
    case TypeId::VARCHAR:
    case TypeId::CHAR: {
      // Truncate string if it's longer than the index key size
      std::string s = val.GetAsString();
      size_t len = std::min(s.length(), KeySize);
      memcpy(data_, s.c_str(), len);
      break;
    }
    default:
      // TIMESTAMP or INVALID: Default to 0
      break;
    }
  }

  // Equality operator (Memcmp is fast)
  inline bool operator==(const GenericKey &other) const {
    return memcmp(data_, other.data_, KeySize) == 0;
  }
};

/**
 * GenericComparator uses the TypeId to decide HOW to compare the bytes.
 * This ensures INTEGER/BIGINT/DECIMAL are sorted numerically,
 * and CHAR/VARCHAR are sorted lexicographically.
 */
template <size_t KeySize> class GenericComparator {
public:
  TypeId type_id_;

  explicit GenericComparator(TypeId type) : type_id_(type) {}

  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    switch (type_id_) {
    case TypeId::BOOLEAN:
    case TypeId::TINYINT: {
      int8_t l, r;
      std::memcpy(&l, lhs.data_, sizeof(int8_t));
      std::memcpy(&r, rhs.data_, sizeof(int8_t));
      if (l < r) return -1;
      if (l > r) return 1;
      return 0;
    }
    case TypeId::SMALLINT: {
      int16_t l, r;
      std::memcpy(&l, lhs.data_, sizeof(int16_t));
      std::memcpy(&r, rhs.data_, sizeof(int16_t));
      if (l < r) return -1;
      if (l > r) return 1;
      return 0;
    }
    case TypeId::INTEGER: {
      int32_t l, r;
      std::memcpy(&l, lhs.data_, sizeof(int32_t));
      std::memcpy(&r, rhs.data_, sizeof(int32_t));
      if (l < r) return -1;
      if (l > r) return 1;
      return 0;
    }
    case TypeId::BIGINT: {
      int64_t l, r;
      std::memcpy(&l, lhs.data_, sizeof(int64_t));
      std::memcpy(&r, rhs.data_, sizeof(int64_t));
      if (l < r)
        return -1;
      if (l > r)
        return 1;
      return 0;
    }
    case TypeId::DECIMAL: {
      double l, r;
      std::memcpy(&l, lhs.data_, sizeof(double));
      std::memcpy(&r, rhs.data_, sizeof(double));
      if (l < r)
        return -1;
      if (l > r)
        return 1;
      return 0;
    }
    case TypeId::VARCHAR:
    case TypeId::CHAR:
    default: {
      // String comparison (lexicographical)
      return memcmp(lhs.data_, rhs.data_, KeySize);
    }
    }
  }
};

} // namespace tetodb