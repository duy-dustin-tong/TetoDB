//CommandParser.h

#pragma once

#include <string>
#include <vector>

struct ParsedCommand {
    std::string type; // CREATE, INSERT, SELECT, DROP, DELETE
    std::string tableName;
    std::vector<std::string> args;
    bool isValid;
    std::string errorMessage;
};

class CommandParser {
public:
    static ParsedCommand Parse(const std::string& line);
};