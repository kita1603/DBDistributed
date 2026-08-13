#pragma once

#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace distdb {

enum class ColumnType {
    kText,
    kInt,
};

struct ColumnDef {
    std::string name;
    ColumnType type;
    bool primary_key = false;
};

struct CreateTableStatement {
    std::string table_name;
    std::vector<ColumnDef> columns;
};

// INSERT always specifies every column by name (no NULLs, no partial
// rows) - `columns[i]` names the column that `values[i]` (raw literal
// text) belongs to; the executor reorders these into schema order.
struct InsertStatement {
    std::string table_name;
    std::vector<std::string> columns;
    std::vector<std::string> values;
};

enum class CompareOp {
    kEq,
    kNe,
    kLt,
    kLe,
    kGt,
    kGe,
};

struct Condition {
    std::string column;
    CompareOp op;
    std::string literal;
};

struct SelectStatement {
    std::string table_name;
    std::vector<std::string> columns;  // empty means "*"
    std::vector<Condition> where;      // empty means no filter; multiple conditions are ANDed
};

struct UpdateStatement {
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> assignments;  // column -> literal
    std::vector<Condition> where;                                  // empty means every row
};

struct DeleteStatement {
    std::string table_name;
    std::vector<Condition> where;  // empty means every row
};

using Statement = std::variant<CreateTableStatement, InsertStatement, SelectStatement, UpdateStatement,
                                DeleteStatement>;

}  // namespace distdb
