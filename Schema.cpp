// Schema.cpp


#include "Schema.h"
#include "Btree.h"
#include "Pager.h"


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
    while(!freeList.empty()) freeList.pop();
}

Table::~Table(){
    for(Column* c : schema) delete c;
    schema.clear();
    
    for(auto const& [colName, indexPager] : indexPagers) {
        delete indexPager;
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
}

void* Table::RowSlot(uint32_t rowId){
    uint32_t pageNum = rowId / rowsPerPage;

    void* page = pager->GetPage(pageNum);

    if(page == nullptr) return nullptr;

    uint16_t offset = rowId % rowsPerPage * rowSize;

    return (char*)page + offset;
}

void Table::CreateIndex(const string& columnName){
    string indexFileName = metaName+"_"+tableName+"_"+columnName+".btree";
    Pager* p = new Pager(indexFileName);

    indexPagers[columnName] = p;

    if(p->numPages == 0){
        LeafNode* rootNode = (LeafNode*)p->GetPage(0);
        InitializeLeafNode(rootNode);
        rootNode->header.isRoot = 1;
        //p->Flush(0, PAGE_SIZE);
    }
    
}

bool Table::IsRowDeleted(uint32_t rowId){
    void* slot = RowSlot(rowId);
    if (!slot) return true;
    uint8_t flag = *(uint8_t*)slot;
    return (flag == 1);
}

void Table::MarkRowDeleted(uint32_t rowId){
    void* slot = RowSlot(rowId);
    if (!slot) return;

    uint8_t flag = 1; // 1 = Dead
    memcpy(slot, &flag, sizeof(uint8_t));

    freeList.push(rowId);
}

uint32_t Table::GetNextRowId() {
    if (!freeList.empty()) {
        uint32_t id = freeList.top();
        freeList.pop();
        return id;
    }
    return rowCount++;
}


