// parser.h

#pragma once

#include <vector>
#include <memory>
#include <stdexcept>
#include "parser/lexer.h"
#include "parser/ast.h"

namespace tetodb {

    class Parser {
    public:
        explicit Parser(const std::vector<Token>& tokens);

        std::unique_ptr<ASTNode> ParseStatement();
        

    private:
        std::vector<Token> tokens_;
        size_t cursor_{ 0 };

        // Parsers
        std::unique_ptr<SelectStatement> ParseSelect();
        std::unique_ptr<InsertStatement> ParseInsert();
        std::unique_ptr<UpdateStatement> ParseUpdate();
        std::unique_ptr<DeleteStatement> ParseDelete();

        // --- Core Grammar Rules ---
        std::unique_ptr<Expr> ParseExpression();
        std::unique_ptr<Expr> ParseBaseExpression(); // Handles columns and numbers
        std::unique_ptr<TableRef> ParseTableRef();
        std::unique_ptr<JoinNode> ParseJoin();
        std::unique_ptr<CreateTableStatement> ParseCreateTable();
        std::unique_ptr<CreateIndexStatement> ParseCreateIndex();
        std::unique_ptr<DropTableStatement> ParseDropTable(); // <-- ADDED
        

        // --- Token Helpers ---
        const Token& Peek(int offset = 0) const;
        const Token& Advance();
        bool Match(TokenType type, const std::string& value = "");
        void Consume(TokenType type, const std::string& error_msg);
    };

} // namespace tetodb