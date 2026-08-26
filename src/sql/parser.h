#pragma once

#include <string>
#include <utility>
#include <vector>

#include "ast.h"
#include "lexer.h"

namespace distdb {

// Recursive-descent parser for a small SQL subset: CREATE DATABASE,
// CREATE TABLE, ALTER TABLE ADD COLUMN, INSERT, SELECT, UPDATE, DELETE,
// SHOW TABLES, SHOW DATABASES. Every table reference must be written
// <database>.<table> (see CreateTableStatement's comment - there's no
// `USE`/current-database concept). WHERE is limited to an AND-chain of
// `column <op> literal` comparisons - no OR, no parentheses in
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
    // `<database>.<table>` - every table reference in this SQL subset
    // (see CreateTableStatement's comment). Throws if there's no `.`,
    // rather than treating a bare identifier as some implicit database,
    // since that implicit-default is exactly the ambient state this
    // project's replicated-log design can't safely support.
    std::pair<std::string, std::string> ExpectQualifiedTableName();

    CreateDatabaseStatement ParseCreateDatabase();
    CreateTableStatement ParseCreateTable();
    AlterTableAddColumnStatement ParseAlterTable();
    InsertStatement ParseInsert();
    SelectStatement ParseSelect();
    UpdateStatement ParseUpdate();
    DeleteStatement ParseDelete();
    ShowTablesStatement ParseShowTables();
    ShowDatabasesStatement ParseShowDatabases();
    std::vector<Condition> ParseWhereClause();
    CompareOp ParseCompareOp();
    ColumnType ParseColumnType();

    std::vector<Token> tokens_;
    size_t pos_ = 0;
};

}  // namespace distdb
