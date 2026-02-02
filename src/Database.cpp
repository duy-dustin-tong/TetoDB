// Database.cpp

#include "tetodb/Database.h"
#include "tetodb/Schema.h"
#include "tetodb/Btree.h"
#include "tetodb/Pager.h"

#include <fstream>
#include <iostream>
#include <algorithm> // for sort


std::unique_ptr<Database> Database::instance = nullptr;

Database& Database::GetInstance()
{
    if (!instance)
    {
        throw std::runtime_error("Database must be initialized with a name first!");
    }
    return *instance;
}

void Database::InitInstance(const std::string& name)
{
    if (!instance)
    {
        instance = std::unique_ptr<Database>(new Database(name));
    }
}

Database::Database(const string& name)
    : metaFileName(name), running(true) 
{
    LoadFromMeta();
}

Database::~Database(){
    for(auto const& [name, table] : tables) delete table;
    tables.clear();
    running = false;
}

Result Database::CreateTable(const string& tableName, stringstream& ss){
    if(tables.find(tableName) != tables.end()){
        cout << "Error: Table " << tableName << " already exists." << std::endl;
        return Result::TABLE_ALREADY_EXISTS;
    }
    string dbFileName = metaFileName+"_"+tableName + ".db";
    remove(dbFileName.c_str());
    
    Table* t = new Table(tableName, metaFileName);
    
    tables[tableName] = t;

    string name, type;
    uint32_t size, offset;
    offset = Table::ROW_HEADER_SIZE;
    

    while(ss >> name >> type >> size){
        if(type == "int"){
            t->AddColumn(new Column(name, INT, 4, offset));
            if(size) t->CreateIndex(name);
            size = 4;
        }

        else t->AddColumn(new Column(name, STRING, size, offset));

        offset+=size;
    }

    return Result::OK;
}

Table* Database::GetTable(const string& name){
    if(tables.count(name)) return tables[name];
    return nullptr;
}

Result Database::DropTable(const string& name){
    if(tables.find(name) == tables.end()) return Result::TABLE_NOT_FOUND;
    
    delete tables[name]; 
    tables.erase(name);

    return Result::OK;
}

Result Database::Insert(const string& name, stringstream& ss){
    Table* t = GetTable(name);
    if(!t) return Result::TABLE_NOT_FOUND;

    Row* r = t->ParseRow(ss);

    if(!r) return Result::INVALID_SCHEMA;
    
    t->Insert(r);

    delete r;

    return Result::OK;
}

void Database::SelectAll(Table* t, vector<Row*>& res){

    res.clear();
    
    for(uint32_t i = 0;i<t->rowCount; i++){
        if(t->IsRowDeleted(i)) continue;
        Row* r = new Row(t->schema);
        void* slot = t->RowSlot(i, 0);
        t->DeserializeRow(slot, r);
        res.push_back(r);
    }
}

uint32_t Database::DeleteAll(Table* t){
    uint32_t deletedCount = 0;
    for(uint32_t i = 0;i<t->rowCount; i++){
        if(t->IsRowDeleted(i)) continue;
        t->MarkRowDeleted(i);
        deletedCount++;
    }
    return deletedCount;
}

void Database::SelectWithRange(Table* t, const string& columnName, void* L, void* R, vector<Row*>& res){

    res.clear();
    vector<uint32_t> selectedRowIds;
    selectedRowIds.clear();
    t->SelectRange(columnName, L, R, selectedRowIds);

    sort(selectedRowIds.begin(), selectedRowIds.end());

    for(uint32_t i : selectedRowIds){
        Row* r = new Row(t->schema);
        void* slot = t->RowSlot(i, 0);
        t->DeserializeRow(slot, r);
        res.push_back(r);
    }

}

uint32_t Database::DeleteWithRange(Table* t, const string& columnName, void* L, void* R){
    return t->DeleteRange(columnName, L, R);
}

void Database::FlushToMeta() {
    ofstream ofs(metaFileName+".teto");
    if(!ofs.is_open()) return;

    ofs << tables.size() << endl;
    for(auto const& [name, table] : tables){
        ofs << name << " " << table->rowCount << " " << table->schema.size() << endl;
        for(Column* c : table->schema){
            if(c->type==INT){
                bool hasIndex = table->colIdx.count(c->columnName);
                ofs << c->columnName << " " << (uint8_t)c->type << " " << hasIndex << " " << c->offset << endl;
            }
            else ofs << c->columnName << " " << (uint8_t)c->type << " " << c->size << " " << c->offset << endl;
        }
        ofs << table->freeList.size() << endl;
        for(uint32_t i : table->freeList) ofs << i << " ";
        ofs << endl;
    }
    ofs.close();
}

void Database::LoadFromMeta(){
    ifstream ifs(metaFileName+".teto");
    if(!ifs.is_open()) return;

    uint32_t numTables;
    if(!(ifs >> numTables)) return;

    for(uint32_t i = 0; i < numTables; i++){
        string tName;
        uint32_t rCount, colCount;
        ifs >> tName >> rCount >> colCount;

        Table* t = new Table(tName, metaFileName, rCount);
        for(uint32_t j = 0; j < colCount; j++){
            string cName;
            uint8_t cTypeInt;
            uint32_t cSize, cOffset;
            ifs >> cName >> cTypeInt >> cSize >> cOffset;
            if(cTypeInt == INT){
                bool hasIndex = cSize;
                t->AddColumn(new Column(cName, (Type)cTypeInt, 4, cOffset));
                if(hasIndex) t->CreateIndex(cName);
            }
            else t->AddColumn(new Column(cName, (Type)cTypeInt, cSize, cOffset));
        }

        uint32_t freeListSize;
        if (ifs >> freeListSize) {
            for(uint32_t k=0; k<freeListSize; k++) {
                uint32_t id;
                ifs >> id;
                t->freeList.push_back(id);
            }
        }

        tables[tName] = t;
    }
    ifs.close();
}

void Database::Commit(){
    FlushToMeta();

    for(auto const& [name, table] : tables){
        if(table->pager) table->pager->FlushAll();

        for(auto const& [col, tree] : table->colIdx){
            if(tree) tree->FlushAll();
        }
    }
}