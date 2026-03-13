#include <gtest/gtest.h>
#include "type/value.h"
#include "type/type_id.h"
#include <string>

namespace tetodb {
TEST(LikeBugTest, PreventStackOverflow) {
    // 5000 character string of 'a's ending in 'b'
    std::string long_str(5000, 'a');
    long_str += "b";

    // 1000 "%a" wildcard patterns ending in "%b"
    std::string pattern;
    for (int i = 0; i < 1000; i++) {
        pattern += "%a";
    }
    pattern += "%b";

    Value val1(TypeId::VARCHAR, long_str);
    Value val2(TypeId::VARCHAR, pattern);

    // This should return true and not crash/overflow the stack
    EXPECT_TRUE(val1.CompareLike(val2));

    // Wait, let's also ensure a non-matching pattern works correctly
    std::string pattern_fail;
    for (int i = 0; i < 1000; i++) {
        pattern_fail += "%a";
    }
    pattern_fail += "%c"; 
    
    Value val3(TypeId::VARCHAR, pattern_fail);
    EXPECT_FALSE(val1.CompareLike(val3));
}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
