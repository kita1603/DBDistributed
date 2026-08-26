#pragma once

#include <string>
#include <vector>

namespace distdb {

enum class TokenType {
    kIdentifier,  // includes keywords - the parser checks text case-insensitively
    kNumber,
    kString,  // text is the literal's contents with quotes stripped
    kSymbol,  // ( ) , ; * = != < <= > >= .
    kEnd,
};

struct Token {
    TokenType type;
    std::string text;
    // Byte offset into the original SQL string where this token started -
    // nothing in the parser itself needs this (it only ever consumes
    // tokens in order), but a caller that wants to rewrite the *original*
    // text at a specific token's position (see raftui's auto-qualify
    // feature in src/ui/main.cpp) needs somewhere to splice, and
    // `text` alone doesn't say where it came from.
    size_t offset = 0;
};

// Throws std::runtime_error on an unterminated string literal or an
// unrecognized character.
std::vector<Token> Tokenize(const std::string& sql);

}  // namespace distdb
