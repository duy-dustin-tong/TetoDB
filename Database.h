// Database.h

#pragma once

#include "Common.h"

using namespace std;

class Table;
class Row;


class Database{
public:
    Database(const string& name);
    ~Database();

    Result CreateTable(const string& tableName, stringstream & ss);
    Table* GetTable(const string& name);
    Result DropTable(const string& name);
    Result Insert(const string& name, stringstream& ss);
    void SelectAll(Table* t, vector<Row*> &res);
    uint32_t DeleteAll(Table* t);
    void SelectWithRange(Table* t, const string& columnName, int32_t L, int32_t R, vector<Row*>& res);
    uint32_t DeleteWithRange(Table* t, const string& columnName, int32_t L, int32_t R);
    void Commit();
    void LoadFromMeta();
    void FlushToMeta();

public:
    map<string, Table*> tables;
    const string metaFileName;
    bool running;
};