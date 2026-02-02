// Database.h

#pragma once

#include "Common.h"
#include <map>
#include <sstream> // Needed for stringstream ref in signatures
#include <memory> // for smart pointers

using namespace std;

class Table;
class Row;


class Database{
public:
    static Database& GetInstance();
    static void InitInstance(const std::string& name);
    
    ~Database();

    Result CreateTable(const string& tableName, stringstream & ss);
    Table* GetTable(const string& name);
    Result DropTable(const string& name);
    Result Insert(const string& name, stringstream& ss);
    void SelectAll(Table* t, vector<Row*> &res);
    uint32_t DeleteAll(Table* t);
    void SelectWithRange(Table* t, const string& columnName, void* L, void* R, vector<Row*>& res);
    uint32_t DeleteWithRange(Table* t, const string& columnName, void* L, void* R);
    void Commit();
    void LoadFromMeta();
    void FlushToMeta();

public:
    map<string, Table*> tables;
    const string metaFileName;
    bool running;

private:
    Database(const string& name);
    static std::unique_ptr<Database> instance;
};