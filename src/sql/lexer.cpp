#include "lexer.h"

#include <cctype>
#include <stdexcept>

namespace distdb {

namespace {

bool IsIdentStart(char c) { return std::isalpha(static_cast<unsigned char>(c)) || c == '_'; }
bool IsIdentChar(char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; }

}  // namespace

std::vector<Token> Tokenize(const std::string& sql) {
    std::vector<Token> tokens;
    size_t i = 0;
    const size_t n = sql.size();

    while (i < n) {
        char c = sql[i];

        if (std::isspace(static_cast<unsigned char>(c))) {
            i++;
            continue;
        }

        if (IsIdentStart(c)) {
            size_t start = i;
            while (i < n && IsIdentChar(sql[i])) i++;
            tokens.push_back({TokenType::kIdentifier, sql.substr(start, i - start)});
            continue;
        }

        if (std::isdigit(static_cast<unsigned char>(c))) {
            size_t start = i;
            while (i < n && std::isdigit(static_cast<unsigned char>(sql[i]))) i++;
            tokens.push_back({TokenType::kNumber, sql.substr(start, i - start)});
            continue;
        }

        if (c == '-' && i + 1 < n && std::isdigit(static_cast<unsigned char>(sql[i + 1]))) {
            size_t start = i;
            i++;
            while (i < n && std::isdigit(static_cast<unsigned char>(sql[i]))) i++;
            tokens.push_back({TokenType::kNumber, sql.substr(start, i - start)});
            continue;
        }

        if (c == '\'') {
            i++;  // skip the opening quote
            std::string value;
            while (true) {
                if (i >= n) throw std::runtime_error("unterminated string literal");
                if (sql[i] == '\'') {
                    if (i + 1 < n && sql[i + 1] == '\'') {  // '' inside a string means a literal '
                        value.push_back('\'');
                        i += 2;
                        continue;
                    }
                    i++;  // closing quote
                    break;
                }
                value.push_back(sql[i]);
                i++;
            }
            tokens.push_back({TokenType::kString, value});
            continue;
        }

        if (c == '<' && i + 1 < n && sql[i + 1] == '=') {
            tokens.push_back({TokenType::kSymbol, "<="});
            i += 2;
            continue;
        }
        if (c == '>' && i + 1 < n && sql[i + 1] == '=') {
            tokens.push_back({TokenType::kSymbol, ">="});
            i += 2;
            continue;
        }
        if (c == '!' && i + 1 < n && sql[i + 1] == '=') {
            tokens.push_back({TokenType::kSymbol, "!="});
            i += 2;
            continue;
        }
        if (c == '<' && i + 1 < n && sql[i + 1] == '>') {  // <> is standard SQL for !=
            tokens.push_back({TokenType::kSymbol, "!="});
            i += 2;
            continue;
        }

        if (c == '(' || c == ')' || c == ',' || c == ';' || c == '*' || c == '=' || c == '<' || c == '>') {
            tokens.push_back({TokenType::kSymbol, std::string(1, c)});
            i++;
            continue;
        }

        throw std::runtime_error(std::string("unexpected character in SQL: '") + c + "'");
    }

    tokens.push_back({TokenType::kEnd, ""});
    return tokens;
}

}  // namespace distdb
