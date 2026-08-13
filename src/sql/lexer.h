#pragma once

#include <string>
#include <vector>

namespace distdb {

enum class TokenType {
    kIdentifier,  // includes keywords - the parser checks text case-insensitively
    kNumber,
    kString,  // text is the literal's contents with quotes stripped
    kSymbol,  // ( ) , ; * = != < <= > >=
    kEnd,
};

struct Token {
    TokenType type;
    std::string text;
};

// Throws std::runtime_error on an unterminated string literal or an
// unrecognized character.
std::vector<Token> Tokenize(const std::string& sql);

}  // namespace distdb
