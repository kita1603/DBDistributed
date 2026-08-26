#include "parser.h"

#include <algorithm>
#include <cctype>
#include <stdexcept>

namespace distdb {

namespace {

std::string ToUpper(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::toupper(c); });
    return out;
}

}  // namespace

Parser::Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

const Token& Parser::Peek() const { return tokens_[pos_]; }
const Token& Parser::Advance() { return tokens_[pos_++]; }

bool Parser::CheckKeyword(const std::string& keyword) const {
    return Peek().type == TokenType::kIdentifier && ToUpper(Peek().text) == keyword;
}

bool Parser::CheckSymbol(const std::string& symbol) const {
    return Peek().type == TokenType::kSymbol && Peek().text == symbol;
}

void Parser::ExpectKeyword(const std::string& keyword) {
    if (!CheckKeyword(keyword)) {
        throw std::runtime_error("expected keyword '" + keyword + "' but found '" + Peek().text + "'");
    }
    Advance();
}

void Parser::ExpectSymbol(const std::string& symbol) {
    if (!CheckSymbol(symbol)) {
        throw std::runtime_error("expected '" + symbol + "' but found '" + Peek().text + "'");
    }
    Advance();
}

std::string Parser::ExpectIdentifier() {
    if (Peek().type != TokenType::kIdentifier) {
        throw std::runtime_error("expected an identifier but found '" + Peek().text + "'");
    }
    return Advance().text;
}

std::string Parser::ExpectLiteral() {
    if (Peek().type != TokenType::kString && Peek().type != TokenType::kNumber) {
        throw std::runtime_error("expected a literal value but found '" + Peek().text + "'");
    }
    return Advance().text;
}

std::pair<std::string, std::string> Parser::ExpectQualifiedTableName() {
    std::string db_name = ExpectIdentifier();
    if (!CheckSymbol(".")) {
        throw std::runtime_error("table name '" + db_name +
                                  "' must be qualified as <database>.<table> - there is no current/default "
                                  "database in this project (see CREATE DATABASE)");
    }
    Advance();
    std::string table_name = ExpectIdentifier();
    return {db_name, table_name};
}

Statement Parser::ParseStatement() {
    if (CheckKeyword("CREATE")) {
        // Peek past CREATE to tell DATABASE apart from TABLE - both start
        // the same way, so ParseStatement is the only place that needs to
        // know which of the two follows.
        if (pos_ + 1 < tokens_.size() && tokens_[pos_ + 1].type == TokenType::kIdentifier &&
            ToUpper(tokens_[pos_ + 1].text) == "DATABASE") {
            return ParseCreateDatabase();
        }
        return ParseCreateTable();
    }
    if (CheckKeyword("ALTER")) return ParseAlterTable();
    if (CheckKeyword("INSERT")) return ParseInsert();
    if (CheckKeyword("SELECT")) return ParseSelect();
    if (CheckKeyword("UPDATE")) return ParseUpdate();
    if (CheckKeyword("DELETE")) return ParseDelete();
    if (CheckKeyword("SHOW")) {
        // Same peek-ahead reasoning as CREATE above, for TABLES vs DATABASES.
        if (pos_ + 1 < tokens_.size() && tokens_[pos_ + 1].type == TokenType::kIdentifier &&
            ToUpper(tokens_[pos_ + 1].text) == "DATABASES") {
            return ParseShowDatabases();
        }
        return ParseShowTables();
    }
    throw std::runtime_error("unrecognized statement starting with '" + Peek().text + "'");
}

CreateDatabaseStatement Parser::ParseCreateDatabase() {
    ExpectKeyword("CREATE");
    ExpectKeyword("DATABASE");

    CreateDatabaseStatement stmt;
    stmt.name = ExpectIdentifier();
    if (CheckSymbol(";")) Advance();
    return stmt;
}

ColumnType Parser::ParseColumnType() {
    std::string type_name = ToUpper(ExpectIdentifier());
    if (type_name == "TEXT") return ColumnType::kText;
    if (type_name == "INT") return ColumnType::kInt;
    throw std::runtime_error("unknown column type '" + type_name + "' (expected TEXT or INT)");
}

CreateTableStatement Parser::ParseCreateTable() {
    ExpectKeyword("CREATE");
    ExpectKeyword("TABLE");

    CreateTableStatement stmt;
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;
    ExpectSymbol("(");

    bool has_pk = false;
    while (true) {
        ColumnDef col;
        col.name = ExpectIdentifier();
        col.type = ParseColumnType();

        if (CheckKeyword("PRIMARY")) {
            Advance();
            ExpectKeyword("KEY");
            if (has_pk) throw std::runtime_error("table cannot have more than one PRIMARY KEY column");
            col.primary_key = true;
            has_pk = true;
        }

        stmt.columns.push_back(col);

        if (CheckSymbol(",")) {
            Advance();
            continue;
        }
        break;
    }
    ExpectSymbol(")");

    if (!has_pk) {
        throw std::runtime_error("table '" + stmt.table_name + "' must declare exactly one PRIMARY KEY column");
    }
    if (CheckSymbol(";")) Advance();
    return stmt;
}

AlterTableAddColumnStatement Parser::ParseAlterTable() {
    ExpectKeyword("ALTER");
    ExpectKeyword("TABLE");

    AlterTableAddColumnStatement stmt;
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;
    ExpectKeyword("ADD");
    if (CheckKeyword("COLUMN")) Advance();  // COLUMN is optional, same as real SQL dialects

    stmt.column.name = ExpectIdentifier();
    stmt.column.type = ParseColumnType();

    // No NULLs in this project (see InsertStatement's comment) and no row
    // rewrite happens here (see SqlExecutor::ExecuteAlterTableAddColumn) -
    // DEFAULT is what every pre-existing row reads back as for this column,
    // so unlike real SQL it isn't optional. Type-checked by the executor,
    // same as every other literal the parser hands off (e.g. INSERT/UPDATE
    // values) - the parser itself never validates literal contents.
    ExpectKeyword("DEFAULT");
    stmt.column.default_value = ExpectLiteral();

    if (CheckSymbol(";")) Advance();
    return stmt;
}

InsertStatement Parser::ParseInsert() {
    ExpectKeyword("INSERT");
    ExpectKeyword("INTO");

    InsertStatement stmt;
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;

    ExpectSymbol("(");
    while (true) {
        stmt.columns.push_back(ExpectIdentifier());
        if (CheckSymbol(",")) {
            Advance();
            continue;
        }
        break;
    }
    ExpectSymbol(")");

    ExpectKeyword("VALUES");
    ExpectSymbol("(");
    while (true) {
        stmt.values.push_back(ExpectLiteral());
        if (CheckSymbol(",")) {
            Advance();
            continue;
        }
        break;
    }
    ExpectSymbol(")");

    if (stmt.columns.size() != stmt.values.size()) {
        throw std::runtime_error("column count does not match value count");
    }
    if (CheckSymbol(";")) Advance();
    return stmt;
}

SelectStatement Parser::ParseSelect() {
    ExpectKeyword("SELECT");

    SelectStatement stmt;
    if (CheckSymbol("*")) {
        Advance();
    } else {
        while (true) {
            stmt.columns.push_back(ExpectIdentifier());
            if (CheckSymbol(",")) {
                Advance();
                continue;
            }
            break;
        }
    }

    ExpectKeyword("FROM");
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;

    if (CheckKeyword("WHERE")) {
        Advance();
        stmt.where = ParseWhereClause();
    }

    if (CheckSymbol(";")) Advance();
    return stmt;
}

UpdateStatement Parser::ParseUpdate() {
    ExpectKeyword("UPDATE");

    UpdateStatement stmt;
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;
    ExpectKeyword("SET");

    while (true) {
        std::string column = ExpectIdentifier();
        ExpectSymbol("=");
        std::string literal = ExpectLiteral();
        stmt.assignments.emplace_back(column, literal);
        if (CheckSymbol(",")) {
            Advance();
            continue;
        }
        break;
    }

    if (CheckKeyword("WHERE")) {
        Advance();
        stmt.where = ParseWhereClause();
    }

    if (CheckSymbol(";")) Advance();
    return stmt;
}

DeleteStatement Parser::ParseDelete() {
    ExpectKeyword("DELETE");
    ExpectKeyword("FROM");

    DeleteStatement stmt;
    auto [db_name, table_name] = ExpectQualifiedTableName();
    stmt.db_name = db_name;
    stmt.table_name = table_name;

    if (CheckKeyword("WHERE")) {
        Advance();
        stmt.where = ParseWhereClause();
    }

    if (CheckSymbol(";")) Advance();
    return stmt;
}

ShowTablesStatement Parser::ParseShowTables() {
    ExpectKeyword("SHOW");
    ExpectKeyword("TABLES");
    ExpectKeyword("FROM");

    ShowTablesStatement stmt;
    stmt.db_name = ExpectIdentifier();
    if (CheckSymbol(";")) Advance();
    return stmt;
}

ShowDatabasesStatement Parser::ParseShowDatabases() {
    ExpectKeyword("SHOW");
    ExpectKeyword("DATABASES");
    if (CheckSymbol(";")) Advance();
    return {};
}

CompareOp Parser::ParseCompareOp() {
    if (Peek().type != TokenType::kSymbol) throw std::runtime_error("expected a comparison operator");
    std::string op = Advance().text;
    if (op == "=") return CompareOp::kEq;
    if (op == "!=") return CompareOp::kNe;
    if (op == "<") return CompareOp::kLt;
    if (op == "<=") return CompareOp::kLe;
    if (op == ">") return CompareOp::kGt;
    if (op == ">=") return CompareOp::kGe;
    throw std::runtime_error("unknown comparison operator '" + op + "'");
}

std::vector<Condition> Parser::ParseWhereClause() {
    std::vector<Condition> conditions;
    while (true) {
        Condition cond;
        cond.column = ExpectIdentifier();
        cond.op = ParseCompareOp();
        cond.literal = ExpectLiteral();
        conditions.push_back(cond);

        if (CheckKeyword("AND")) {
            Advance();
            continue;
        }
        break;
    }
    return conditions;
}

}  // namespace distdb
