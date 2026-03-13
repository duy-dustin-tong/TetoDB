#include <gtest/gtest.h>
#include "server/tetodb_instance.h"
#include <filesystem>

namespace tetodb {
TEST(AggregationBugTest, EmptyTableAggregations) {
    if (std::filesystem::exists("test_agg_cpp.db")) {
        std::filesystem::remove("test_agg_cpp.db");
    }

    TetoDBInstance db("test_agg_cpp.db");
    ClientSession session;

    db.ExecuteQuery("CREATE TABLE emptytbl (id INTEGER PRIMARY KEY, val INTEGER)", session);

    // Test COUNT(*)
    auto r1 = db.ExecuteQuery("SELECT COUNT(*) FROM emptytbl", session);
    EXPECT_EQ(r1.rows.size(), 1);
    if (r1.rows.size() == 1) {
        EXPECT_EQ(r1.rows[0].GetValue(r1.schema, 0).GetAsInteger(), 0);
    }

    // Test SUM(val)
    auto r2 = db.ExecuteQuery("SELECT SUM(val) FROM emptytbl", session);
    EXPECT_EQ(r2.rows.size(), 1);
    if (r2.rows.size() == 1) {
        EXPECT_TRUE(r2.rows[0].GetValue(r2.schema, 0).IsNull());
    }

    // Test GROUP BY
    auto r3 = db.ExecuteQuery("SELECT val, COUNT(*) FROM emptytbl GROUP BY val", session);
    EXPECT_EQ(r3.rows.size(), 0);
}
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
