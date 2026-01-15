// Schema.cpp

#include "Schema.h"

Column::Column(const string &name, Type type, size_t size, size_t offset)
    : columnName(name), type(type), size(size), offset(offset) {}

Column::~Column(){}

Row::Row(const vector<Column*> &schema){
    for(Column* c : schema){
        if(c->type == INT) value[c->columnName] = malloc(sizeof(int));
        else value[c->columnName] = malloc(c->size);
    }
}   

Row::~Row(){
    for(auto &p : value) free(p.second);
}

Table::Table(const string &name) 
    : tableName(name), rowCount(0), rowSize(0)
{
    for(int i = 0;i<MAX_PAGE;i++) pages[i] = nullptr;
}

Table::~Table(){
    for(Column* c : schema) delete c;
    schema.clear();

    for(int i = 0;i<MAX_PAGE;i++) free(pages[i]), pages[i] = nullptr;
}

Row* Table::ParseRow(stringstream &ss){
    string str;
    int num;

    Row* r = new Row(schema);
    for(Column* c : schema){
        if(c->type == INT){
            ss >> num;
            *(int*)(r->value[c->columnName]) = num;
        }
        else{
            ss >> quoted(str);
            size_t len = min(str.size(), c->size-1);
            void* dest = r->value[c->columnName];
            memset(dest, 0, c->size);
            memcpy(dest, &str[0], len+1);
        }
    }
    return r;
}

void Table::SerializeRow(Row* src, void* dest){
    for(Column* c : schema){
        memset(dest, 0, c->size);
        memcpy(dest, src->value[c->columnName], c->size);
        dest = (char*)dest + c->size;
    }
}

void Table::DeserializeRow(void* src, Row* dest){
    for(Column* c : schema){
        memcpy(dest->value[c->columnName], src, c->size);
        src = (char*)src + c->size;
    }
}

void Table::AddColumn(Column* c){
    schema.push_back(c);
    rowSize += c->size;
    rowsPerPage = PAGE_SIZE / rowSize;
    maxRows = rowsPerPage*MAX_PAGE;
}

void* Table::RowSlot(int rowNum){
    int pageNum = rowNum / rowsPerPage;

    if(pageNum >= MAX_PAGE) return nullptr;

    void* page = pages[pageNum];

    if(page == nullptr) page = pages[pageNum] = malloc(PAGE_SIZE);

    int offset = rowNum % rowsPerPage * rowSize;

    return (char*)page + offset;
}



Database::Database() {}

Database::~Database(){
    for(auto const& [name, table] : tables) delete table;
    tables.clear();
}

Result Database::CreateTable(const string& tableName, stringstream& ss){
    if(tables.find(tableName) != tables.end()){
        cout << "Error: Table " << tableName << " already exists." << std::endl;
        return Result::TABLE_ALREADY_EXISTS;
    }
    Table* t = new Table(tableName);
    tables[tableName] = t;

    string name, type;
    size_t size, offset;
    offset = 0;
    

    while(ss >> name >> type >> size){
        if(type == "int") t->AddColumn(new Column(name, INT, size, offset));
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

    if(t->rowCount >= t->maxRows) return Result::OUT_OF_STORAGE;

    void* slot = t->RowSlot(t->rowCount);
    if(!slot) return Result::OUT_OF_STORAGE;

    Row* r = t->ParseRow(ss);
    t->SerializeRow(r, slot);
    t->rowCount++;

    delete r;
    return Result::OK;
}

vector<Row*> Database::SelectAll(const string &name){
    Table* t = GetTable(name);
    if(!t) return {};

    vector<Row*> res;
    res.clear();

    for(int i = 0; i < t->rowCount; i++){
        void* slot = t->RowSlot(i);
        Row* r = new Row(t->schema);
        t->DeserializeRow(slot, r);
        res.push_back(r);
    }

    return res;
}