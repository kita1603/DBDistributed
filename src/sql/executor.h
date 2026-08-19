#pragma once

#include <optional>
#include <string>
#include <vector>

#include "../storage/engine.h"
#include "ast.h"
#include "schema.h"

namespace distdb {

// Executes parsed SQL statements against a StorageEngine, which only
// knows about opaque string keys/values - this layer maps SQL tables and
// rows onto that key space:
//   "__schema__/<table>"   -> serialized column list (TableSchema)
//   "__row__/<table>/<pk>" -> serialized column values (EncodeRow)
//
// There is no secondary indexing: SELECT/UPDATE/DELETE without a filter
// on the primary key still scan every row under a table's prefix via
// StorageEngine::Scan.
//
// ALTER TABLE ADD COLUMN only ever touches the schema entry, never
// existing rows - a row written before the column existed is shorter than
// the current schema, and is padded out with the new column's mandatory
// DEFAULT the moment it's next read (see PadRow in executor.cpp), not
// rewritten up front.
class SqlExecutor {
 public:
    explicit SqlExecutor(StorageEngine& engine);

    // Parses and executes a single SQL statement. Returns text meant to
    // be printed directly (e.g. "OK", "2 row(s) updated", or a
    // tab-separated SELECT result table). Throws std::runtime_error on
    // any parse or execution error.
    std::string Execute(const std::string& sql);

    // If `stmt` deterministically touches exactly one row - an INSERT
    // (whose primary key value is right there in the statement) or a
    // SELECT/UPDATE/DELETE whose WHERE pins the primary key with an
    // equality condition - returns that row's storage key
    // ("__row__/<table>/<pk-value>"). Returns nullopt for anything else
    // (CREATE TABLE, or a WHERE clause that doesn't pin the primary key,
    // e.g. a full table scan or a filter on a non-key column) - the
    // caller must then treat the statement as touching the whole table
    // rather than routing it to a single shard. Used by the sharding
    // layer (raft_main.cpp), which has no other way to know which shard
    // a statement belongs to without duplicating schema knowledge.
    std::optional<std::string> TryExtractRowKey(const Statement& stmt) const;

 private:
    std::string ExecuteCreateTable(const CreateTableStatement& stmt);
    std::string ExecuteAlterTableAddColumn(const AlterTableAddColumnStatement& stmt);
    std::string ExecuteInsert(const InsertStatement& stmt);
    std::string ExecuteSelect(const SelectStatement& stmt);
    std::string ExecuteUpdate(const UpdateStatement& stmt);
    std::string ExecuteDelete(const DeleteStatement& stmt);

    TableSchema LoadSchema(const std::string& table_name) const;
    static std::string SchemaKey(const std::string& table_name);
    static std::string RowPrefix(const std::string& table_name);
    static std::string RowKey(const std::string& table_name, const std::string& pk_value);

    static bool MatchesWhere(const TableSchema& schema, const std::vector<std::string>& row,
                              const std::vector<Condition>& where);
    static bool CompareLiterals(ColumnType type, const std::string& lhs, CompareOp op, const std::string& rhs);
    static void ValidateIntLiteral(const std::string& column, const std::string& literal);

    StorageEngine& engine_;
};

}  // namespace distdb
