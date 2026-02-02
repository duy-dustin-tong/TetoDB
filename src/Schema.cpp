// Schema.cpp

#include "tetodb/Schema.h"
#include "tetodb/Btree.h"  // Needed for CreateIndex logic
#include "tetodb/Pager.h"  // Needed for Pager methods

#include <cstring>
#include <iostream>
#include <iomanip> // for quoted
#include <sstream>


Column::Column(const string &name, Type type, uint32_t size, uint32_t offset)
    : columnName(name), type(type), size(size), offset(offset) {}

Column::~Column(){}

Row::Row(const vector<Column*> &schema){
    for(Column* c : schema){
        if(c->type == INT) value[c->columnName] = malloc(sizeof(int32_t));
        else value[c->columnName] = malloc(c->size);
    }
}   

Row::~Row(){
    for(auto &p : value) free(p.second);
}

Table::Table(const string &name, const string &meta) 
    : tableName(name), rowCount(0), rowSize(ROW_HEADER_SIZE), rowsPerPage(0), metaName(meta), pager(new Pager(meta+"_"+name+".db"))
{}

Table::Table(const string &name, const string &meta, uint32_t rowCount) 
    : tableName(name), rowCount(rowCount), rowSize(ROW_HEADER_SIZE), metaName(meta), pager(new Pager(meta+"_"+name+".db"))
{
    while(!freeList.empty()) freeList.pop_back();
}

Table::~Table(){
    for(Column* c : schema) delete c;
    schema.clear();
    
    for(auto const& [colName, indexing] : colIdx){
        delete indexing;
    }
    delete pager;
}

Row* Table::ParseRow(stringstream &ss){
    string str;
    int32_t num;

    Row* r = new Row(schema);
    for(Column* c : schema){
        if(c->type == INT){
            ss >> num;
            *(int32_t*)(r->value[c->columnName]) = num;
        }
        else{
            ss >> quoted(str);
            uint32_t len = min((uint32_t)str.size(), c->size-1);
            void* dest = r->value[c->columnName];
            memset(dest, 0, c->size);
            memcpy(dest, &str[0], len);
        }
    }
    return r;
}

void Table::SerializeRow(Row* src, void* dest){
    if(src == nullptr || dest == nullptr) return;

    uint8_t isDeleted = 0;
    memcpy(dest, &isDeleted, sizeof(uint8_t));
    

    for(Column* c : schema){
        memset((char*)dest+c->offset, 0, c->size);
        memcpy((char*)dest+c->offset, src->value[c->columnName], c->size);
        
    }
}

void Table::DeserializeRow(void* src, Row* dest){
    if(src == nullptr || dest == nullptr) return;
    

    for(Column* c : schema){
        memcpy(dest->value[c->columnName], (char*)src+c->offset, c->size);
        
    }
}

void Table::AddColumn(Column* c){
    schema.push_back(c);
    rowSize += c->size;
    rowsPerPage = PAGE_SIZE / rowSize; 
    colPtr[c->columnName] = c;
}

void* Table::RowSlot(uint32_t rowId, bool markDirty){
    uint32_t pageNum = rowId / rowsPerPage;

    void* page = pager->GetPage(pageNum, markDirty);

    if(page == nullptr) return nullptr;

    uint16_t offset = rowId % rowsPerPage * rowSize;

    return (char*)page + offset;
}

void Table::CreateIndex(const string& columnName){
    if(colPtr.find(columnName)==colPtr.end()){
        cout << "Error: Column '" << columnName << "' not found." << endl;
        return;
    }

    Column* col = colPtr[columnName];

    string indexFileName = metaName + "_" + tableName + "_" + columnName + ".btree";
    Pager* p = new Pager(indexFileName);


    BtreeIndex* tree = nullptr;

    switch(col->type){
        case INT: tree = new Btree<int32_t>(p, this); break;
        // other cases
    }


    if(p->numPages == 0){
        tree->CreateIndex(); 
    }
    colIdx[columnName] = tree;
}

bool Table::IsRowDeleted(uint32_t rowId){
    void* slot = RowSlot(rowId, 0);
    if (!slot) return true;
    uint8_t flag = *(uint8_t*)slot;
    return (flag == 1);
}

void Table::MarkRowDeleted(uint32_t rowId){
    void* slot = RowSlot(rowId, 1);
    if (!slot) return;

    uint8_t flag = 1; // 1 = Dead
    memcpy(slot, &flag, sizeof(uint8_t));

    freeList.push_back(rowId);
}

uint32_t Table::GetNextRowId(){
    if(!freeList.empty()){
        uint32_t id = freeList.back();
        freeList.pop_back();
        return id;
    }
    return rowCount++;
}

void Table::Insert(Row* r){
    uint32_t newRowId = GetNextRowId();
    
    for(Column* c : schema){
        if(colIdx.find(c->columnName) != colIdx.end()){
            colIdx[c->columnName]->Insert(r->value[c->columnName], newRowId);
        }
    }


    SerializeRow(r, RowSlot(newRowId, 1));
}

void Table::SelectRange(const string& colName, void* L, void* R, vector<uint32_t>& out){
    // if has index
    if(colIdx.find(colName) != colIdx.end()){
        BtreeIndex* tree = colIdx[colName];
        tree->SelectRange(L, R, out);
        return;
    }

    if(colPtr.find(colName) == colPtr.end()){
        cout << "Error: Column " << colName << " not found." << endl;
        return;
    }

    Column* col = colPtr[colName];
    switch(col->type){
        case INT: SelectScan<int32_t>(col, L, R, out); break;
        // other cases
    }
}

uint32_t Table::DeleteRange(const string& colName, void* L, void* R){
    // if has index
    if(colIdx.find(colName) != colIdx.end()){
        BtreeIndex* tree = colIdx[colName];
        return tree->DeleteRange(L, R);;
    }

    if(colPtr.find(colName) == colPtr.end()){
        cout << "Error: Column " << colName << " not found." << endl;
        return 0;
    }

    Column* col = colPtr[colName];
    switch(col->type){
        case INT: return DeleteScan<int32_t>(col, L, R);
        // other cases
    }

    return 0;
}

template <typename T>
void Table::SelectScan(Column* col, void* L, void* R, vector<uint32_t>& out){
    T valL = *(T*) L;
    T valR = *(T*) R;

    for(uint32_t i = 0; i < rowCount; i++){
        if(IsRowDeleted(i)) continue;

        void* slot = RowSlot(i, 0);
        void* colData = (char*)slot + col->offset;
        T key = *(T*) colData;

        if(valL <= key && key <= valR){
            out.push_back(i);
        }
    }
}

template <typename T>
uint32_t Table::DeleteScan(Column* col, void* L, void* R){
    uint32_t deletedCount = 0;
    T valL = *(T*) L;
    T valR = *(T*) R;

    for(uint32_t i = 0; i < rowCount; i++){
        if(IsRowDeleted(i)) continue;

        void* slot = RowSlot(i, 0);
        void* colData = (char*)slot + col->offset;
        T key = *(T*) colData;

        if(valL <= key && key <= valR){
            MarkRowDeleted(i);
            deletedCount++;
        }
    }
    return deletedCount;
}


