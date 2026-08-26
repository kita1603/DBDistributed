#pragma once

#include <optional>
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
    // Only ever set for a column added later via ALTER TABLE ADD COLUMN -
    // a column declared in the original CREATE TABLE is never missing from
    // a row (INSERT must specify every column that existed at insert time),
    // so it never needs one. Mandatory for ALTER TABLE specifically because
    // this project has no NULL concept (see InsertStatement's own comment)
    // - an existing row that predates the column has to read back as
    // *something*, and a default is the only value that can mean.
    std::optional<std::string> default_value;
};

// A database, unlike a table, is never itself schema stored anywhere - it
// exists purely as a `__database__/<name>` marker key (see
// SqlExecutor::ExecuteCreateDatabase) that CREATE TABLE checks for before
// allowing a table into it. Broadcast to every shard the same way CREATE
// TABLE is (a table in this database could end up on any shard).
struct CreateDatabaseStatement {
    std::string name;
};

// Every table reference in this project's SQL is mandatorily qualified as
// <database>.<table> - there is no "current database"/`USE` concept,
// deliberately: a bare, unqualified name would have to be resolved
// against some ambient session state, but every proposed command is
// replicated as plain opaque text with nothing else recorded alongside it
// (see InsertStatement's own comment on the same underlying reason INSERT
// can't have implicit/partial columns either) - a follower applying that
// text later has no session to resolve it against, and two different
// connections could disagree about "current database" for the exact same
// text. Qualifying every reference inline sidesteps the question
// entirely: the command means the same thing to everyone who ever reads
// it back, forever.
struct CreateTableStatement {
    std::string db_name;
    std::string table_name;
    std::vector<ColumnDef> columns;
};

// Schema-only change, broadcast to every shard the same way CreateTable is
// (see raft_main.cpp) - existing rows aren't rewritten; SqlExecutor pads
// them with `column.default_value` lazily, the first time each is read
// after the ALTER, rather than rewriting the whole table up front.
struct AlterTableAddColumnStatement {
    std::string db_name;
    std::string table_name;
    ColumnDef column;
};

// INSERT always specifies every column by name (no NULLs, no partial
// rows) - `columns[i]` names the column that `values[i]` (raw literal
// text) belongs to; the executor reorders these into schema order.
struct InsertStatement {
    std::string db_name;
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
    std::string db_name;
    std::string table_name;
    std::vector<std::string> columns;  // empty means "*"
    std::vector<Condition> where;      // empty means no filter; multiple conditions are ANDed
};

struct UpdateStatement {
    std::string db_name;
    std::string table_name;
    std::vector<std::pair<std::string, std::string>> assignments;  // column -> literal
    std::vector<Condition> where;                                  // empty means every row
};

struct DeleteStatement {
    std::string db_name;
    std::string table_name;
    std::vector<Condition> where;  // empty means every row
};

// Lists every table's name in one database - schema-only, like
// CreateTableStatement, but a *read*: CREATE/ALTER TABLE already broadcast
// a table's schema to every shard (see raft_main.cpp), so any single
// shard's own copy is already the complete, correct answer for that
// database - no scatter/gather needed the way a row-data SELECT with no
// WHERE would need. Executed and routed exactly like a SelectStatement
// (see raft_main.cpp/raftui's is_read checks), just against a fixed query
// rather than a table's rows. `FROM <database>` is mandatory for the same
// reason every table reference is (see CreateTableStatement's comment).
struct ShowTablesStatement {
    std::string db_name;
};

// Lists every database's name - the one place in this SQL subset that
// doesn't take a qualified name, since a database *is* the thing being
// named. Same routing/read reasoning as ShowTablesStatement.
struct ShowDatabasesStatement {};

using Statement = std::variant<CreateTableStatement, InsertStatement, SelectStatement, UpdateStatement,
                                DeleteStatement, AlterTableAddColumnStatement, ShowTablesStatement,
                                CreateDatabaseStatement, ShowDatabasesStatement>;

}  // namespace distdb
