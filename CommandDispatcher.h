// CommandDispatcher.h

#pragma once

#include "Common.h"

class Database;
class Table;
class Row;

void PrintTable(const vector<Row*>& rows, Table* t);
void ExecuteCommand(const string &line);
void ProcessDotCommand(const string &line);