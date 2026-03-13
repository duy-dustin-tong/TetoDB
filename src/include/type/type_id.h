// type.h

#pragma once

#include <cstdint>

namespace tetodb {

    enum class TypeId {
        INVALID = 0,
        BOOLEAN,
        TINYINT,
        SMALLINT,
        INTEGER,
        BIGINT,
        DECIMAL,
        VARCHAR,
        CHAR, 
        TIMESTAMP
    };

} // namespace tetodb