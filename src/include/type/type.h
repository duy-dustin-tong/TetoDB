// type.h

#pragma once

#include <string>
#include "type/value.h"
#include "type/type_id.h"

namespace tetodb {

    /**
     * Type is the abstract base class for all type logic.
     * It replaces the switch statements in Tuple.cpp.
     */
    class Type {
    public:
        explicit Type(TypeId type_id) : type_id_(type_id) {}
        virtual ~Type() = default;

        // --- Singleton Access ---
        static const Type* GetInstance(TypeId type_id);

        // --- Core Operations ---

        // 1. Get the fixed length of this type (if constant).
        // Returns 0 if variable (VARCHAR) or user-defined (CHAR).
        virtual uint32_t GetFixedLength() const = 0;

        // 2. Is this type inlined in the tuple header?
        virtual bool IsInlined() const = 0;

        // 3. Serialize to a fixed-length slot (The Header).
        // For INT: Writes 4 bytes.
        // For CHAR(N): Writes N bytes + padding.
        // 'max_length' is the schema constraint (e.g. N for CHAR, 0 for INT).
        virtual void SerializeTo(const Value& val, char* storage, uint32_t max_length) const = 0;

        // 4. Deserialize from a fixed-length slot.
        virtual Value DeserializeFrom(const char* storage, uint32_t max_length) const = 0;

        // 5. Serialize to the Heap (For VARCHAR).
        // Returns number of bytes written.
        virtual uint32_t SerializeToHeap(const Value& val, char* storage) const {
            // Default: Most types don't use the heap.
            return 0;
        }

        // 6. Debug
        virtual std::string ToString(const Value& val) const {
            return val.ToString();
        }

    protected:
        TypeId type_id_;
    };

} // namespace tetodb