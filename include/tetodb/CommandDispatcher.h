// CommandDispatcher.h

#pragma once

#include <string>
#include <vector>

using namespace std;

// Forward decl
class Table; 
class Row;


void PrintTable(const vector<Row*>& rows, Table* t);
void ExecuteCommand(const string &line);
void ProcessDotCommand(const string &line);