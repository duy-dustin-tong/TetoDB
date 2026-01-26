// CommandDispatcher.cpp

#include "Database.h"
#include "Schema.h"
#include "CommandDispatcher.h"
#include "CommandParser.h"

extern Database* DB_INSTANCE;

void PrintTable(const vector<Row*>& rows, Table* t) {
    if (rows.empty()) {
        cout << "Empty set." << endl;
        return;
    }

    // 1. Calculate column widths
    vector<int> widths;
    for (Column* c : t->schema) {
        widths.push_back(max((int)c->columnName.length(), (int)c->size)); 
    }

    // 2. Print Header
    cout << "+";
    for (int w : widths) cout << string(w + 2, '-') << "+";
    cout << endl << "|";
    
    for (uint32_t i = 0; i < t->schema.size(); i++) {
        cout << " " << left << setw(widths[i]) << t->schema[i]->columnName << " |";
    }
    cout << endl << "+";
    for (int w : widths) cout << string(w + 2, '-') << "+";
    cout << endl;

    // 3. Print Rows
    for (Row* r : rows) {
        cout << "|";
        for (uint32_t i = 0; i < t->schema.size(); i++) {
            Column* c = t->schema[i];
            if (c->type == INT) {
                cout << " " << left << setw(widths[i]) << *(int*)r->value[c->columnName] << " |";
            } else {
                cout << " " << left << setw(widths[i]) << (char*)r->value[c->columnName] << " |";
            }
        }
        cout << endl;
        delete r; // Clean up row after printing
    }

    // 4. Print Footer
    cout << "+";
    for (int w : widths) cout << string(w + 2, '-') << "+";
    cout << endl;
    cout << rows.size() << " rows in set." << endl;
}

void ProcessDotCommand(const string &line){
    stringstream ss;
    ss << line;

    string cmd, tableName;

    ss >> cmd;

    if(cmd == ".exit"){
        DB_INSTANCE->running = false;
        return;
    }
    if(cmd == ".commit") {
        DB_INSTANCE->Commit();
        cout << "Changes committed to disk." << endl;
        return;
    }
    if(cmd == ".help"){
        cout << "Read the readme, i aint helping lol" << endl;
        return;
    }
    if(cmd == ".tables"){
        for(auto &[name, table] : DB_INSTANCE->tables) cout << name << endl;
        return;
    }
    if(cmd == ".schema"){
        ss >> tableName;
        Table* t = DB_INSTANCE->GetTable(tableName);

        if(t == nullptr){
            cout << "Name does not exist" << endl;
            return;
        }

        for(Column* c : t->schema){
            cout<<c->columnName<<' '<<c->type<<' '<<c->size<<' '<<c->offset<<endl;
        }

        return;

    }
} 

void ExecuteCommand(const string &line){
    if(line.empty()) return;

    if(line[0] == '.') { ProcessDotCommand(line); return; }

    ParsedCommand cmd = CommandParser::Parse(line);

    if (!cmd.isValid) {
        cout << cmd.errorMessage << endl;
        return;
    }

    if (cmd.type == "CREATE") {
        // You might need to adjust CreateTable to take vector<string> args instead of stringstream
        // Or reconstruct a stringstream here to keep existing logic working
        stringstream ss; 
        for(auto& s : cmd.args) ss << s << " ";
        
        Result res = DB_INSTANCE->CreateTable(cmd.tableName, ss);
        if (res == Result::OK) cout << "Query OK: Table '" << cmd.tableName << "' created." << endl;
        else cout << "Error: Could not create table." << endl;
    }
    else if (cmd.type == "INSERT") {
        // FIX: Fetch table FIRST to check column types
        Table* t = DB_INSTANCE->GetTable(cmd.tableName);
        if(!t) { 
            cout << "Error: Table '" << cmd.tableName << "' not found." << endl; 
            return; 
        }

        stringstream ss;
        for(uint32_t i = 0; i < cmd.args.size(); i++){
             // Check if we are within schema bounds
             if(i < t->schema.size()) {
                 // ONLY quote if it is a STRING. Integers must be raw.
                 if(t->schema[i]->type == INT) {
                     ss << cmd.args[i] << " ";
                 } else {
                     ss << quoted(cmd.args[i]) << " ";
                 }
             }
        }
        
        Result res = DB_INSTANCE->Insert(cmd.tableName, ss);
        if (res == Result::OK) cout << "Query OK: 1 row inserted." << endl;
        else if (res == Result::INVALID_SCHEMA) cout << "Error: Data type mismatch." << endl;
        else cout << "Error: Insert failed." << endl;
    }
    else if (cmd.type == "SELECT") {
        Table* t = DB_INSTANCE->GetTable(cmd.tableName);
        if (!t) { cout << "Error: Table '" << cmd.tableName << "' not found." << endl; return; }

        vector<Row*> rows;
        if (cmd.args.empty()) {
            DB_INSTANCE->SelectAll(t, rows);
        } else {
            // Args are [col, min, max]
            string col = cmd.args[0];
            int32_t l = stoi(cmd.args[1]);
            int32_t r = stoi(cmd.args[2]);
            DB_INSTANCE->SelectWithRange(t, col, l, r, rows);
        }
        
        PrintTable(rows, t);
    }
    else if (cmd.type == "DELETE") {
        Table* t = DB_INSTANCE->GetTable(cmd.tableName);
        if (!t) { cout << "Error: Table '" << cmd.tableName << "' not found." << endl; return; }

        uint32_t deletedCount = 0;
        if (cmd.args.empty()) {
            deletedCount = DB_INSTANCE->DeleteAll(t);
        } else {
            // Args are [col, min, max]
            string col = cmd.args[0];
            int32_t l = stoi(cmd.args[1]);
            int32_t r = stoi(cmd.args[2]);
            deletedCount = DB_INSTANCE->DeleteWithRange(t, col, l, r);
        }

        cout<<"Deleted " << deletedCount << " rows."<<endl;
    }

}