// CommandDispatcher.cpp

#include "CommandDispatcher.h"

Database DB;

Result ProcessCommand(string &line){
    stringstream ss;
    ss << line;

    string cmd, keyword, tableName;

    ss >> cmd >> keyword >> tableName;

    if(cmd == "create") return DB.CreateTable(tableName, ss);
    if(cmd == "insert") return DB.Insert(tableName, ss);
    if(cmd == "drop") return DB.DropTable(tableName);
    if(cmd == "select") {
        Table* t = DB.GetTable(tableName);

        if(t==nullptr) return Result::TABLE_NOT_FOUND;

        vector<Row*> rows = DB.SelectAll(tableName);

        for(Row* r : rows){
            for(Column* c : t->schema){
                if(c->type == INT) cout << *(int*)(r->value[c->columnName]) << " | ";
                else cout << '"' << (char*)(r->value[c->columnName]) << "\" | ";
            }
            cout << endl;
        }

        return Result::OK;
    }

    return Result::ERROR;
} 

void ProcessDotCommand(string &line){
    stringstream ss;
    ss << line;

    string cmd, tableName;

    ss >> cmd;

    if(cmd == ".exit") exit(0);
    if(cmd == ".help") cout << "Read the docs, i aint helping" << endl;
    if(cmd == ".tables"){
        for(auto &[name, table] : DB.tables) cout << name << endl;
    }
    if(cmd == ".schema"){
        ss >> tableName;
        Table* t = DB.GetTable(tableName);

        if(t == nullptr){
            cout << "Name does not exist" << endl;
            return;
        }

        for(Column* c : t->schema){
            cout<<c->columnName<<' '<<c->type<<' '<<c->size<<' '<<c->offset<<endl;
        }

    }
} 