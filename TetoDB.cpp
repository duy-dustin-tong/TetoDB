//TetoDB.cpp

#include <iostream>
#include <fstream>
#include <chrono>
#include <iomanip> // setprecision

#include "Database.h"
#include "CommandDispatcher.h"




void RunCommandWithTimer(const string& line) {
    // 1. Start Timer
    auto start = chrono::high_resolution_clock::now();
    
    // 2. Execute
    ExecuteCommand(line);
    
    // 3. FORCE FLUSH (Crucial for benchmarking output speed)
    cout << flush; 

    // 4. Stop Timer
    auto end = chrono::high_resolution_clock::now();
    
    // 5. Calculate Duration
    chrono::duration<double, milli> elapsed = end - start;
    
    // 6. Print with HIGH PRECISION (6 decimals)
    if(!line.empty()) {
        cout << "(" << fixed << setprecision(6) << elapsed.count() << " ms)" << endl;
    }
}

int main(int argc, char* argv[]){
    if(argc<2){
        cout << "Need filename" <<endl;
        return -1;
    }
    
    string dbName = argv[1];
    Database::InitInstance(dbName);
    auto& dbInstance = Database::GetInstance();

    if(argc>=3){
        string txtFileName = argv[2];
        ifstream txtFile(txtFileName);
        

        if(!txtFile.is_open()){
            cout<<"ERROR: couldnt open commands file"<<endl;
        }
        else{
            string line;
            
            while(getline(txtFile, line) && dbInstance.running){
                RunCommandWithTimer(line);
            }
            
        }
        txtFile.close();
        dbInstance.running = 0;
    }

    while(dbInstance.running){
        cout << "TETO_DB >> ";

        string line;
        getline(cin, line);

        RunCommandWithTimer(line);
        

    }

    cout << "Exiting..." << endl;

    return 0;
}