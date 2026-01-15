//Schema.h

#pragma once

#include <vector>
#include <map>
#include <sstream>
#include "Common.h"

#define MAX_PAGE 100
#define PAGE_SIZE 4096

class Column{
public:
    Column(const string &name, Type type, size_t size, size_t offset);
    ~Column();

public:
    string columnName;
    Type type;
    size_t size;
    size_t offset;
};

class Row{
public:
    Row();
    Row(const vector<Column*> &schema);
    ~Row();

public:
    map<string, void*> value;
};

class Table{
public:
    Table(const string &name);
    ~Table();

    Row* ParseRow(stringstream &ss);
    void SerializeRow(Row* src, void* dest);
    void DeserializeRow(void* src, Row* dest);
    void AddColumn(Column* c);
    void* RowSlot(int rowNum);


public:
    string tableName;
    vector<Column*> schema;
    
    void* pages[MAX_PAGE];

    int rowCount;
    int rowSize;
    int rowsPerPage;
    int maxRows;

};

class Database{
public:
    Database();
    ~Database();

    Result CreateTable(const string& tableName, stringstream & ss);
    Table* GetTable(const string& name);
    Result DropTable(const string& name);
    Result Insert(const string& name, stringstream& ss);
    vector<Row*> SelectAll(const string& name);

public:
    std::map<string, Table*> tables;
};