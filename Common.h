//Common.h

#pragma once

#include <algorithm>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <cstring>
#include <cstdint>
#include <sstream>
#include <iomanip>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fstream>

using namespace std;

enum Type { NONE = -1, INT = 1, STRING };

enum class Result {
    OK,
    TABLE_ALREADY_EXISTS,
    TABLE_NOT_FOUND,
    OUT_OF_STORAGE,
    INVALID_SCHEMA,
    ERROR
};

inline Type GetTypeFromString(string &s){
    if(s == "int") return INT;
    if(s == "char") return STRING;
    return NONE;
}

inline string GetTypeName(Type t){
    switch(t){
        case NONE: return "NONE";
        case INT: return "INT";
        case STRING: return "STRING";
    }
    return "NONE";
}