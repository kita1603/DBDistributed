#pragma once

#include <string>
#include <vector>

#include "ast.h"
#include "lexer.h"

namespace distdb {

// Recursive-descent parser for a small SQL subset: CREATE TABLE, ALTER
// TABLE ADD COLUMN, INSERT, SELECT, UPDATE, DELETE. WHERE is limited to an
// AND-chain of `column <op> literal` comparisons - no OR, no parentheses in
// expressions, no subqueries, no JOINs. Throws std::runtime_error with a
// human-readable message on any syntax error.
class Parser {
 public:
    explicit Parser(std::vector<Token> tokens);

    Statement ParseStatement();

 private:
    const Token& Peek() const;
    const Token& Advance();
    bool CheckKeyword(const std::string& keyword) const;
    bool CheckSymbol(const std::string& symbol) const;
    void ExpectKeyword(const std::string& keyword);
    void ExpectSymbol(const std::string& symbol);
    std::string ExpectIdentifier();
    std::string ExpectLiteral();

    CreateTableStatement ParseCreateTable();
    AlterTableAddColumnStatement ParseAlterTable();
    InsertStatement ParseInsert();
    SelectStatement ParseSelect();
    UpdateStatement ParseUpdate();
    DeleteStatement ParseDelete();
    std::vector<Condition> ParseWhereClause();
    CompareOp ParseCompareOp();
    ColumnType ParseColumnType();

    std::vector<Token> tokens_;
    size_t pos_ = 0;
};

}  // namespace distdb
