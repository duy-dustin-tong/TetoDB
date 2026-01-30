//Schema.h

#pragma once


#include "Common.h"
#include <map>
#include <stack>


class Pager;

using namespace std;

class Column{
public:
    Column(const string &name, Type type, uint32_t size, uint32_t offset);
    ~Column();

public:
    string columnName;
    Type type;
    uint32_t size;
    uint32_t offset;
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
    Table(const string &name, const string &meta); // make new table
    Table(const string &name, const string &meta, uint32_t rowCount); // load table from disk
    ~Table();

    Row* ParseRow(stringstream &ss);
    void SerializeRow(Row* src, void* dest);
    void DeserializeRow(void* src, Row* dest);
    void AddColumn(Column* c);
    void* RowSlot(uint32_t rowId, bool markDirty);

    bool IsRowDeleted(uint32_t rowId);
    void MarkRowDeleted(uint32_t rowId);
    uint32_t GetNextRowId();

    void CreateIndex(const string& columnName);


public:
    string metaName;
    string tableName;
    vector<Column*> schema;
    stack<uint32_t> freeList;
    map<string, Pager*> indexPagers;
    Pager* pager;

    uint32_t rowCount;
    uint16_t rowSize;
    uint16_t rowsPerPage;
    
    static const uint8_t ROW_HEADER_SIZE = 1;

};

