// CommandParser.cpp

#include "CommandParser.h"
#include "Common.h"

#include <sstream>
#include <algorithm>
#include <iomanip> // quoted

ParsedCommand CommandParser::Parse(const string& line) {
    ParsedCommand cmd;
    cmd.isValid = false;
    
    stringstream ss(line);
    string token;
    
    if (!(ss >> token)) return cmd; // Empty line
    
    // Normalize to upper case for checking, but keep original for values
    std::string upperToken = token;
    std::transform(upperToken.begin(), upperToken.end(), upperToken.begin(), ::toupper);

    if (upperToken == "CREATE") {
        std::string keyword;
        ss >> keyword; 
        if (keyword != "TABLE") {
            cmd.errorMessage = "Syntax Error: Expected 'TABLE' after CREATE";
            return cmd;
        }
        if (!(ss >> cmd.tableName)) {
            cmd.errorMessage = "Syntax Error: Missing table name";
            return cmd;
        }
        
        std::string colName, colType;
        uint16_t colSize;
        while (ss >> colName >> colType >> colSize) {
            cmd.args.push_back(colName);
            cmd.args.push_back(colType);
            cmd.args.push_back(to_string(colSize));
        }
        cmd.type = "CREATE";
        cmd.isValid = true;
    } 
    else if (upperToken == "INSERT") {
        std::string keyword;
        ss >> keyword; 
        if (keyword != "INTO") {
             cmd.errorMessage = "Syntax Error: Expected 'INTO' after INSERT";
             return cmd;
        }
        if (!(ss >> cmd.tableName)) {
             cmd.errorMessage = "Syntax Error: Missing table name";
             return cmd;
        }

        // Capture the rest of the line as args
        std::string val;
        while (ss >> quoted(val)) { // Use quoted to handle "strings with spaces" if needed
             cmd.args.push_back(val);
        }
        // Basic check: Insert needs at least one value? (Ideally check against schema later)
        cmd.type = "INSERT";
        cmd.isValid = true;
    }
    else if (upperToken == "SELECT") {
        std::string keyword;
        ss >> keyword; 
        if (keyword != "FROM") {
             cmd.errorMessage = "Syntax Error: Expected 'FROM' after SELECT";
             return cmd;
        }
        if (!(ss >> cmd.tableName)) {
             cmd.errorMessage = "Syntax Error: Missing table name";
             return cmd;
        }
        
        cmd.type = "SELECT";
        cmd.isValid = true;

        std::string whereKw;
        if (ss >> whereKw) {
            if (whereKw == "WHERE") {
                std::string col, l, r;
                if (ss >> col >> l >> r) {
                    cmd.args.push_back(col);
                    cmd.args.push_back(l);
                    cmd.args.push_back(r);
                } else {
                    cmd.isValid = false;
                    cmd.errorMessage = "Syntax Error: WHERE clause needs <col> <min> <max>";
                }
            }
        }
    }
    else if (upperToken == "DELETE") {
        std::string keyword;
        ss >> keyword; 
        if (keyword != "FROM") {
             cmd.errorMessage = "Syntax Error: Expected 'FROM' after DELETE";
             return cmd;
        }
        if (!(ss >> cmd.tableName)) {
             cmd.errorMessage = "Syntax Error: Missing table name";
             return cmd;
        }
        
        cmd.type = "DELETE";
        cmd.isValid = true;

        std::string whereKw;
        if (ss >> whereKw) {
            if (whereKw == "WHERE") {
                string col, l, r;
                if (ss >> col >> l >> r) {
                    cmd.args.push_back(col);
                    cmd.args.push_back(l);
                    cmd.args.push_back(r);
                } else {
                    cmd.isValid = false;
                    cmd.errorMessage = "Syntax Error: WHERE clause needs <col> <min> <max>";
                }
            }
        }
    }
    else {
        cmd.errorMessage = "Unknown command: " + token;
    }

    return cmd;
}