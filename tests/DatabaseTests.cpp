#include "../Database.h"
#include <gtest/gtest.h>
#include <iostream>

/// <summary>
/// If we does not initialize first, we can't get a database instance.
/// </summary>
/// <param name=""></param>
/// <param name=""></param>
TEST(DatabaseTests, EmptyInstanceTest)
{
	EXPECT_ANY_THROW(auto& dbInstance = Database::GetInstance());
}

/// <summary>
/// Initialize a database with a name.
/// </summary>
/// <param name=""></param>
/// <param name=""></param>
TEST(DatabaseTests, InitializeDatabaseInstanceTest)
{
	std::string name = "my_db";
	Database::InitInstance(name);
	auto& dbInstance = Database::GetInstance();
	EXPECT_EQ(dbInstance.metaFileName, "my_db");
}

/// <summary>
/// Because in the previous test case, we already initialize a database 
/// with name my_db, so when we try to intialize a new database with a 
/// different name, it will not work, and return the old instance.
/// </summary>
/// <param name=""></param>
/// <param name=""></param>
TEST(DatabaseTests, SingletonTest)
{
	std::string name = "another_db";
	Database::InitInstance(name);
	auto& dbInstance = Database::GetInstance();
	EXPECT_NE(dbInstance.metaFileName, "another_db");
	EXPECT_EQ(dbInstance.metaFileName, "my_db");
}