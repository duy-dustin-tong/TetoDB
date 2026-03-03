// lexer.h

#pragma once

#include <string>
#include <vector>
#include <unordered_set>

namespace tetodb {

    enum class TokenType {
        KEYWORD, IDENTIFIER, NUMBER, STRING, SYMBOL,
        PRIMARY, KEY,
        PARAMETER, // <-- ADDED
        END_OF_FILE, INVALID
    };

    struct Token {
        TokenType type_;
        std::string value_;


        // Helper to print tokens for debugging
        std::string ToString() const {
            std::string type_str;
            switch (type_) {
            case TokenType::KEYWORD:     type_str = "KEYWORD"; break;
            case TokenType::IDENTIFIER:  type_str = "IDENTIFIER"; break;
            case TokenType::NUMBER:      type_str = "NUMBER"; break;
            case TokenType::STRING:      type_str = "STRING"; break;
            case TokenType::SYMBOL:      type_str = "SYMBOL"; break;
            case TokenType::PRIMARY:     type_str = "PRIMARY"; break;
            case TokenType::KEY:         type_str = "KEY"; break;
            case TokenType::PARAMETER:   type_str = "PARAMETER"; break;
            case TokenType::END_OF_FILE: type_str = "EOF"; break;
            case TokenType::INVALID:     type_str = "INVALID"; break;
            }
            return "[" + type_str + ": " + value_ + "]";
        }
    };

    class Lexer {
    public:
        explicit Lexer(const std::string& input);

        // Extracts the next token from the string
        Token NextToken();

        // Convenience function to extract all tokens at once
        std::vector<Token> TokenizeAll();

    private:
        std::string input_;
        size_t cursor_{ 0 };

        // Helper characters
        char Peek() const;
        char Advance();
        void SkipWhitespace();

        // Token extractors
        Token ReadNumber();
        Token ReadIdentifierOrKeyword();
        Token ReadString();
        Token ReadSymbol();

        // Character classification
        bool IsAlpha(char c) const;
        bool IsDigit(char c) const;
        bool IsWhitespace(char c) const;

        // The list of recognized SQL keywords
        static const std::unordered_set<std::string> keywords_;
    };

} // namespace tetodb