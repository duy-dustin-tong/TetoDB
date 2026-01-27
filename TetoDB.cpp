//TetoDB.cpp

#include <iostream>
#include <fstream>
#include <chrono>
#include <iomanip> // setprecision

#include "Database.h"
#include "CommandDispatcher.h"


Database* DB_INSTANCE = nullptr;

int main(int argc, char* argv[]){
    if(argc<2){
        cout << "Need filename" <<endl;
        return -1;
    }

    string dbName = argv[1];

    DB_INSTANCE = new Database(dbName);

    if(argc>=3){
        string txtFileName = argv[2];
        ifstream txtFile(txtFileName);
        

        if(!txtFile.is_open()){
            cout<<"ERROR: couldnt open commands file"<<endl;
        }
        else{
            string line;
            while(getline(txtFile, line) && DB_INSTANCE->running){
                auto start = chrono::high_resolution_clock::now();
                ExecuteCommand(line);
                auto end = chrono::high_resolution_clock::now();
                
                chrono::duration<double, milli> elapsed = end - start;
                if(!line.empty()) cout << "(" << fixed << setprecision(2) << elapsed.count() << " ms)" << endl;
            }
            
        }
        txtFile.close();
        DB_INSTANCE->running = 0;
    }

    while(DB_INSTANCE->running){
        cout << "TETO_DB >> ";

        string line;
        getline(cin, line);

        auto start = chrono::high_resolution_clock::now();
        ExecuteCommand(line);
        auto end = chrono::high_resolution_clock::now();
        
        chrono::duration<double, milli> elapsed = end - start;
        if(!line.empty()) cout << "(" << fixed << setprecision(2) << elapsed.count() << " ms)" << endl;

    }

    cout << "Exiting..." << endl;
    delete DB_INSTANCE;

    return 0;
}