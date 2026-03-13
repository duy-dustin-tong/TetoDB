#pragma once

#include "common/record_id.h"
#include <memory>

namespace tetodb {

/**
 * Type-erased wrapper for IndexIterator.
 * This allows the abstract `Index` class to return an iterator
 * without needing to know the underlying KeyType/ValueType templates.
 */
class AbstractIndexIterator {
public:
  virtual ~AbstractIndexIterator() = default;

  // Returns true if the iterator has exhausted all tuples in the B+Tree
  virtual bool IsEnd() const = 0;

  // Advances the underlying Iterator (Latch crabbing to next page if necessary)
  virtual void Advance() = 0;

  // Retrieves the RID (ValueType) of the current Tuple
  virtual RID GetCurrentRid() const = 0;

  // Returns true if the current Key is greater than the search bound.
  // This pushes the `GenericComparator` logic down into the subclass
  // where the types are known.
  virtual bool IsPastSearchBound() const = 0;
};

} // namespace tetodb
