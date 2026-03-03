// record_id.h

#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include "common/config.h"

namespace tetodb {

    class RID {
    public:
        RID() = default;

        RID(page_id_t page_id, uint32_t slot_id)
            : page_id_(page_id), slot_id_(slot_id) {
        }

        inline page_id_t GetPageId() const { return page_id_; }
        inline uint32_t GetSlotId() const { return slot_id_; }

        inline void Set(page_id_t page_id, uint32_t slot_id) {
            page_id_ = page_id;
            slot_id_ = slot_id;
        }


        bool operator==(const RID& other) const {
            return page_id_ == other.page_id_ && slot_id_ == other.slot_id_;
        }

        bool operator!=(const RID& other) const {
            return !(*this == other);
        }

        // Debug helper
        std::string ToString() const {
            std::stringstream os;
            os << "RID(Page=" << page_id_ << ", Slot=" << slot_id_ << ")";
            return os.str();
        }

    private:
        page_id_t page_id_{ INVALID_PAGE_ID };
        uint32_t slot_id_{ 0 };
    };

} // namespace tetodb


namespace std {
    template <>
    struct hash<tetodb::RID> {
        size_t operator()(const tetodb::RID& rid) const {
            // Combine the hashes of Page ID and Slot Num
            size_t h1 = std::hash<tetodb::page_id_t>()(rid.GetPageId());
            size_t h2 = std::hash<uint32_t>()(rid.GetSlotId());
            return h1 ^ (h2 << 1); // Bitwise XOR to combine them
        }
    };
}