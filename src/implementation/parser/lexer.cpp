// lexer.cpp

#include "parser/lexer.h"
#include <algorithm> // For std::transform (uppercase conversion)

namespace tetodb {

// Define our recognized keywords (in uppercase for case-insensitive matching)
const std::unordered_set<std::string> Lexer::keywords_ = {
    "SELECT",     "FROM",       "WHERE",   "AS",      "INSERT",    "INTO",
    "VALUES",     "UPDATE",     "SET",     "DELETE",  "CREATE",    "TABLE",
    "INT",        "INTEGER",    "VARCHAR", "JOIN",    "ON",        "AND",
    "OR",         "GROUP",      "BY",      "SUM",     "COUNT",     "AVG",
    "LIMIT",      "OFFSET",     "INDEX",   "UNIQUE",  "PRIMARY",   "KEY",
    "FOREIGN",    "REFERENCES", "CASCADE", "LIKE",    "ILIKE",     "BEGIN",
    "COMMIT",     "ROLLBACK",   "DROP",    "EXPLAIN", "SAVEPOINT", "RELEASE",
    "DEALLOCATE", "TO",         "ALL",     "NULL",    "IS",        "NOT",
    "BOOLEAN",    "DECIMAL",    "FLOAT",   "DATE",    "TIMESTAMP"};

Lexer::Lexer(const std::string &input) : input_(input), cursor_(0) {}

std::vector<Token> Lexer::TokenizeAll() {
  std::vector<Token> tokens;
  while (true) {
    Token t = NextToken();
    tokens.push_back(t);
    if (t.type_ == TokenType::END_OF_FILE || t.type_ == TokenType::INVALID) {
      break;
    }
  }
  return tokens;
}

Token Lexer::NextToken() {
  SkipWhitespace();
  if (cursor_ >= input_.length())
    return {TokenType::END_OF_FILE, ""};

  char c = Peek();

  // --- NEW: Intercept Parameter Tokens ($1, $2) ---
  if (c == '$') {
    Advance(); // Skip '$'
    std::string val;
    while (IsDigit(Peek())) {
      val += Advance();
    }
    if (val.empty())
      return {TokenType::INVALID, "$"};
    return {TokenType::PARAMETER, val};
  }

  if (IsAlpha(c) || c == '_')
    return ReadIdentifierOrKeyword();
  if (IsDigit(c))
    return ReadNumber();
  if (c == '"') {
    // SQL double-quoted identifier (e.g. "_pg3_1")
    Advance(); // Skip opening "
    std::string value;
    while (cursor_ < input_.length() && Peek() != '"') {
      value += Advance();
    }
    if (cursor_ < input_.length() && Peek() == '"') {
      Advance(); // Skip closing "
    }
    return {TokenType::IDENTIFIER, value};
  }
  if (c == '\'')
    return ReadString();

  return ReadSymbol();
}

// ==========================================
// EXTRACTORS
// ==========================================

Token Lexer::ReadIdentifierOrKeyword() {
  std::string value;
  while (cursor_ < input_.length() &&
         (IsAlpha(Peek()) || IsDigit(Peek()) || Peek() == '_')) {
    value += Advance();
  }

  std::string upper_value = value;
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(),
                 ::toupper);

  // ==========================================
  // NEW: Intercept PRIMARY and KEY specifically
  // ==========================================
  if (upper_value == "PRIMARY") {
    return {TokenType::PRIMARY, upper_value};
  }
  if (upper_value == "KEY") {
    return {TokenType::KEY, upper_value};
  }

  // Standardize remaining keywords
  if (keywords_.find(upper_value) != keywords_.end()) {
    return {TokenType::KEYWORD, upper_value};
  }

  return {TokenType::IDENTIFIER, value};
}

Token Lexer::ReadNumber() {
  std::string value;
  bool has_decimal = false;

  while (cursor_ < input_.length() && (IsDigit(Peek()) || Peek() == '.')) {
    if (Peek() == '.') {
      if (has_decimal)
        break; // Can't have two decimals in a number
      has_decimal = true;
    }
    value += Advance();
  }
  return {TokenType::NUMBER, value};
}

Token Lexer::ReadString() {
  std::string value;
  Advance(); // Skip the opening single quote

  while (cursor_ < input_.length() && Peek() != '\'') {
    value += Advance();
  }

  if (cursor_ < input_.length() && Peek() == '\'') {
    Advance(); // Skip the closing single quote
    return {TokenType::STRING, value};
  }

  return {TokenType::INVALID, "UNTERMINATED_STRING"};
}

Token Lexer::ReadSymbol() {
  char c = Advance();
  std::string value(1, c);

  // Handle two-character symbols like <=, >=, !=
  if ((c == '<' || c == '>' || c == '!') && Peek() == '=') {
    value += Advance();
    return {TokenType::SYMBOL, value};
  }

  // Validate basic single-character symbols
  if (c == '=' || c == '<' || c == '>' || c == '*' || c == ',' || c == '(' ||
      c == ')' || c == ';' || c == '.') {
    return {TokenType::SYMBOL, value};
  }

  return {TokenType::INVALID, value};
}

// ==========================================
// HELPERS
// ==========================================

char Lexer::Peek() const {
  if (cursor_ >= input_.length())
    return '\0';
  return input_[cursor_];
}

char Lexer::Advance() {
  if (cursor_ >= input_.length())
    return '\0';
  return input_[cursor_++];
}

void Lexer::SkipWhitespace() {
  while (cursor_ < input_.length()) {
    if (IsWhitespace(Peek())) {
      Advance();
    }
    // --- NEW: Skip SQL Comments (-- ...) ---
    else if (Peek() == '-' && cursor_ + 1 < input_.length() &&
             input_[cursor_ + 1] == '-') {
      // We found '--', advance until the end of the line
      while (cursor_ < input_.length() && Peek() != '\n') {
        Advance();
      }
    } else {
      // Not whitespace, not a comment. We are at a real token!
      break;
    }
  }
}

bool Lexer::IsAlpha(char c) const {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool Lexer::IsDigit(char c) const { return c >= '0' && c <= '9'; }

bool Lexer::IsWhitespace(char c) const {
  return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

} // namespace tetodb