//CommandParser.h

#pragma once

#include <string>
#include <vector>
#include <cctype>
#include <functional>

//namespace more suitable here
namespace CommandParser {
    enum class CommandType{
        Create,
        Insert,
        Select,
        Delete,
        System
    };
    //main struct, all commands inherit from this
    struct Command{
        CommandType type;
        std::string tablename;
        virtual ~Command()=default;
    };
        struct CreateTableCommand : Command{
            struct Column{
                std::string name;
                enum class Type{Int,Char}type;
                int extra;
            };
            std::vector<Column> columns;
        };
        //dont rly understand this one
        struct InsertCommand : Command{
            
        };
        struct SelectCommand : Command{
            bool hasWhere;
            std::string column;
            int min;
            int max;
        };
        struct DeleteCommand : Command{
            bool hasWhere;
            std::string column;
            int min;    //do i need that?
            int max;
        };

    std::unique_ptr<Command> parseCreate(std::vector<std::string> args){

    }
    std::unique_ptr<Command> parseInsert(std::vector<std::string> args){

    }
    std::unique_ptr<Command> parseSelect(std::vector<std::string> args){

    }
    std::unique_ptr<Command> parseDelete(std::vector<std::string> args){

    }
    std::unique_ptr<Command> parseSytem(std::vector<std::string> args){

    }
    inline static const std::map<std::string,std::function<std::unique_ptr<Command>(const std::vector<std::string>&)>> parseFunctions={
        {"CREATE",parseCreate},
        {"INSERT",parseInsert},
        {"SELECT",parseSelect},
        {"DELETE",parseDelete} 
    };

    std::vector<std::string> splitArgs(const std::string& input) {
        std::vector<std::string> args;
        std::string arg;
        bool inQuotes = false;

        for (size_t i = 0; i < input.size(); ++i) {
            char t = input[i];

            if (t == '"') {
                inQuotes = !inQuotes;
                continue;
            }
            // split on whitespace if not inside quotes
            if (!inQuotes && std::isspace(static_cast<unsigned char>(t))) {
                if (!arg.empty()) {
                    args.push_back(arg);
                    arg.clear();
                }
            } else {
                arg += t;
            }
        }
        if (!arg.empty())
            args.push_back(arg);

        return args;
    }
    
    std::unique_ptr<Command> parse(std::string& input){
        auto args = splitArgs(input);
        if(args.empty())return nullptr;
        std::string cmd = args[0];
        std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
        
        //search in map<string,function> if string exists, call respective parse<Type> function
        if(auto it = parseFunctions.find(cmd);it !=parseFunctions.end()){
            return it->second(args);
        }
        //if something went wrong:
        return nullptr;
    }
};