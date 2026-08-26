#include "executor.h"

#include <cctype>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <type_traits>
#include <variant>

#include "lexer.h"
#include "parser.h"

namespace distdb {

namespace {

// Rows can hold arbitrary bytes (unlike column names), so they need a
// real length-prefixed binary encoding rather than the delimited text
// format TableSchema uses.
std::string EncodeRow(const std::vector<std::string>& values) {
    std::string out;
    uint32_t count = static_cast<uint32_t>(values.size());
    out.append(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& v : values) {
        uint32_t len = static_cast<uint32_t>(v.size());
        out.append(reinterpret_cast<const char*>(&len), sizeof(len));
        out.append(v);
    }
    return out;
}

std::vector<std::string> DecodeRow(const std::string& blob) {
    size_t pos = 0;
    auto ReadU32 = [&](uint32_t& out) {
        if (pos + sizeof(uint32_t) > blob.size()) throw std::runtime_error("corrupt row encoding");
        std::memcpy(&out, blob.data() + pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);
    };

    uint32_t count = 0;
    ReadU32(count);
    std::vector<std::string> values;
    values.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t len = 0;
        ReadU32(len);
        if (pos + len > blob.size()) throw std::runtime_error("corrupt row encoding");
        values.emplace_back(blob.substr(pos, len));
        pos += len;
    }
    return values;
}

// A row shorter than `schema.columns` predates one or more ALTER TABLE ADD
// COLUMN calls - pad it out with each missing column's DEFAULT (mandatory
// for exactly this reason, see ColumnDef's comment) so every other
// function can keep indexing `row[idx]` by the *current* schema without
// caring which rows are old and which are new.
void PadRow(const TableSchema& schema, std::vector<std::string>& row) {
    while (row.size() < schema.columns.size()) {
        row.push_back(*schema.columns[row.size()].default_value);
    }
}

// Finds an equality condition on `schema`'s primary key column within
// `where`, if any - shared by TryExtractRowKey's SELECT/UPDATE/DELETE
// cases below.
std::optional<std::string> FindPkEquality(const TableSchema& schema, const std::vector<Condition>& where) {
    int pk_idx = schema.PrimaryKeyIndex();
    if (pk_idx < 0) return std::nullopt;
    const std::string& pk_name = schema.columns[pk_idx].name;
    for (const auto& cond : where) {
        if (cond.column == pk_name && cond.op == CompareOp::kEq) return cond.literal;
    }
    return std::nullopt;
}

}  // namespace

SqlExecutor::SqlExecutor(StorageEngine& engine) : engine_(engine) {}

std::string SqlExecutor::DatabaseKey(const std::string& db_name) { return "__database__/" + db_name; }
std::string SqlExecutor::SchemaKey(const std::string& db_name, const std::string& table_name) {
    return "__schema__/" + db_name + "/" + table_name;
}
std::string SqlExecutor::RowPrefix(const std::string& db_name, const std::string& table_name) {
    return "__row__/" + db_name + "/" + table_name + "/";
}
std::string SqlExecutor::RowKey(const std::string& db_name, const std::string& table_name,
                                 const std::string& pk_value) {
    return RowPrefix(db_name, table_name) + pk_value;
}

void SqlExecutor::RequireDatabaseExists(const std::string& db_name) const {
    if (!engine_.Get(DatabaseKey(db_name))) throw std::runtime_error("no such database: " + db_name);
}

TableSchema SqlExecutor::LoadSchema(const std::string& db_name, const std::string& table_name) const {
    auto blob = engine_.Get(SchemaKey(db_name, table_name));
    if (!blob) throw std::runtime_error("no such table: " + db_name + "." + table_name);
    return TableSchema::Deserialize(*blob);
}

void SqlExecutor::ValidateIntLiteral(const std::string& column, const std::string& literal) {
    if (literal.empty()) throw std::runtime_error("invalid integer value for column '" + column + "'");
    size_t start = (literal[0] == '-') ? 1 : 0;
    if (start == literal.size()) throw std::runtime_error("invalid integer value for column '" + column + "'");
    for (size_t i = start; i < literal.size(); i++) {
        if (!std::isdigit(static_cast<unsigned char>(literal[i]))) {
            throw std::runtime_error("invalid integer value for column '" + column + "': '" + literal + "'");
        }
    }
}

bool SqlExecutor::CompareLiterals(ColumnType type, const std::string& lhs, CompareOp op, const std::string& rhs) {
    int cmp;
    if (type == ColumnType::kInt) {
        long long a = std::stoll(lhs);
        long long b = std::stoll(rhs);
        cmp = (a < b) ? -1 : (a > b ? 1 : 0);
    } else {
        int raw = lhs.compare(rhs);
        cmp = (raw < 0) ? -1 : (raw > 0 ? 1 : 0);
    }
    switch (op) {
        case CompareOp::kEq:
            return cmp == 0;
        case CompareOp::kNe:
            return cmp != 0;
        case CompareOp::kLt:
            return cmp < 0;
        case CompareOp::kLe:
            return cmp <= 0;
        case CompareOp::kGt:
            return cmp > 0;
        case CompareOp::kGe:
            return cmp >= 0;
    }
    return false;
}

bool SqlExecutor::MatchesWhere(const TableSchema& schema, const std::vector<std::string>& row,
                                const std::vector<Condition>& where) {
    for (const auto& cond : where) {
        int idx = schema.ColumnIndex(cond.column);
        if (idx < 0) throw std::runtime_error("no such column: " + cond.column);
        if (!CompareLiterals(schema.columns[idx].type, row[idx], cond.op, cond.literal)) return false;
    }
    return true;
}

std::string SqlExecutor::Execute(const std::string& sql) {
    Parser parser(Tokenize(sql));
    Statement stmt = parser.ParseStatement();
    return std::visit(
        [this](auto&& s) -> std::string {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, CreateDatabaseStatement>) {
                return ExecuteCreateDatabase(s);
            } else if constexpr (std::is_same_v<T, CreateTableStatement>) {
                return ExecuteCreateTable(s);
            } else if constexpr (std::is_same_v<T, AlterTableAddColumnStatement>) {
                return ExecuteAlterTableAddColumn(s);
            } else if constexpr (std::is_same_v<T, InsertStatement>) {
                return ExecuteInsert(s);
            } else if constexpr (std::is_same_v<T, SelectStatement>) {
                return ExecuteSelect(s);
            } else if constexpr (std::is_same_v<T, UpdateStatement>) {
                return ExecuteUpdate(s);
            } else if constexpr (std::is_same_v<T, ShowTablesStatement>) {
                return ExecuteShowTables(s);
            } else if constexpr (std::is_same_v<T, ShowDatabasesStatement>) {
                return ExecuteShowDatabases(s);
            } else {
                return ExecuteDelete(s);
            }
        },
        stmt);
}

std::optional<std::string> SqlExecutor::TryExtractRowKey(const Statement& stmt) const {
    return std::visit(
        [this](auto&& s) -> std::optional<std::string> {
            using T = std::decay_t<decltype(s)>;
            try {
                if constexpr (std::is_same_v<T, InsertStatement>) {
                    TableSchema schema = LoadSchema(s.db_name, s.table_name);
                    int pk_idx = schema.PrimaryKeyIndex();
                    const std::string& pk_name = schema.columns[pk_idx].name;
                    for (size_t i = 0; i < s.columns.size(); i++) {
                        if (s.columns[i] == pk_name) return RowKey(s.db_name, s.table_name, s.values[i]);
                    }
                    return std::nullopt;  // INSERT didn't name the pk column - ExecuteInsert will report that
                } else if constexpr (std::is_same_v<T, SelectStatement> || std::is_same_v<T, UpdateStatement> ||
                                      std::is_same_v<T, DeleteStatement>) {
                    TableSchema schema = LoadSchema(s.db_name, s.table_name);
                    auto pk = FindPkEquality(schema, s.where);
                    if (pk) return RowKey(s.db_name, s.table_name, *pk);
                    return std::nullopt;  // no WHERE <pk> = ... - touches the whole table
                } else {
                    return std::nullopt;  // CreateDatabaseStatement/CreateTableStatement/
                                           // AlterTableAddColumnStatement - schema, not a row, needs every
                                           // shard; ShowTablesStatement/ShowDatabasesStatement - a read, but
                                           // not pinned to any single row either
                }
            } catch (...) {
                // e.g. the table doesn't exist - let the normal
                // execution path (which the caller falls back to)
                // report that error instead of this one going unnoticed.
                return std::nullopt;
            }
        },
        stmt);
}

std::string SqlExecutor::ExecuteCreateDatabase(const CreateDatabaseStatement& stmt) {
    if (engine_.Get(DatabaseKey(stmt.name))) {
        throw std::runtime_error("database already exists: " + stmt.name);
    }
    // The value itself carries no information - existence of the key is
    // the only thing that matters (RequireDatabaseExists just checks
    // engine_.Get(...).has_value()) - so an empty string is fine; it's
    // not a tombstone (see StorageEngine's own doc comment), just a
    // zero-length live value.
    engine_.Put(DatabaseKey(stmt.name), "");
    return "OK";
}

std::string SqlExecutor::ExecuteCreateTable(const CreateTableStatement& stmt) {
    RequireDatabaseExists(stmt.db_name);
    if (engine_.Get(SchemaKey(stmt.db_name, stmt.table_name))) {
        throw std::runtime_error("table already exists: " + stmt.db_name + "." + stmt.table_name);
    }
    TableSchema schema;
    schema.columns = stmt.columns;
    engine_.Put(SchemaKey(stmt.db_name, stmt.table_name), schema.Serialize());
    return "OK";
}

std::string SqlExecutor::ExecuteAlterTableAddColumn(const AlterTableAddColumnStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.db_name, stmt.table_name);

    if (schema.ColumnIndex(stmt.column.name) >= 0) {
        throw std::runtime_error("column already exists: " + stmt.column.name);
    }
    // The parser requires DEFAULT, so this should always hold - but the
    // executor is the layer that actually enforces literal validity (same
    // division as INSERT/UPDATE's ValidateIntLiteral calls), not the parser.
    if (!stmt.column.default_value) throw std::runtime_error("ALTER TABLE ADD COLUMN requires a DEFAULT value");
    if (stmt.column.type == ColumnType::kInt) ValidateIntLiteral(stmt.column.name, *stmt.column.default_value);

    schema.columns.push_back(stmt.column);
    engine_.Put(SchemaKey(stmt.db_name, stmt.table_name), schema.Serialize());
    return "OK";
}

std::string SqlExecutor::ExecuteInsert(const InsertStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.db_name, stmt.table_name);

    if (stmt.columns.size() != schema.columns.size()) {
        throw std::runtime_error("INSERT must specify all " + std::to_string(schema.columns.size()) +
                                  " column(s) of table '" + stmt.db_name + "." + stmt.table_name + "'");
    }

    std::vector<std::string> row(schema.columns.size());
    std::vector<bool> filled(schema.columns.size(), false);
    for (size_t i = 0; i < stmt.columns.size(); i++) {
        int idx = schema.ColumnIndex(stmt.columns[i]);
        if (idx < 0) throw std::runtime_error("no such column: " + stmt.columns[i]);
        if (filled[idx]) throw std::runtime_error("column specified twice: " + stmt.columns[i]);
        if (schema.columns[idx].type == ColumnType::kInt) ValidateIntLiteral(stmt.columns[i], stmt.values[i]);
        row[idx] = stmt.values[i];
        filled[idx] = true;
    }

    int pk_idx = schema.PrimaryKeyIndex();
    std::string key = RowKey(stmt.db_name, stmt.table_name, row[pk_idx]);
    if (engine_.Get(key)) throw std::runtime_error("duplicate primary key: " + row[pk_idx]);

    engine_.Put(key, EncodeRow(row));
    return "OK";
}

std::string SqlExecutor::ExecuteSelect(const SelectStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.db_name, stmt.table_name);

    std::vector<int> projection;
    std::vector<std::string> headers;
    if (stmt.columns.empty()) {
        for (size_t i = 0; i < schema.columns.size(); i++) {
            projection.push_back(static_cast<int>(i));
            headers.push_back(schema.columns[i].name);
        }
    } else {
        for (const auto& name : stmt.columns) {
            int idx = schema.ColumnIndex(name);
            if (idx < 0) throw std::runtime_error("no such column: " + name);
            projection.push_back(idx);
            headers.push_back(name);
        }
    }

    std::ostringstream out;
    for (size_t i = 0; i < headers.size(); i++) {
        if (i > 0) out << '\t';
        out << headers[i];
    }

    size_t match_count = 0;
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.db_name, stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
        PadRow(schema, row);
        if (!MatchesWhere(schema, row, stmt.where)) continue;
        match_count++;
        out << '\n';
        for (size_t i = 0; i < projection.size(); i++) {
            if (i > 0) out << '\t';
            out << row[projection[i]];
        }
    }
    if (match_count == 0) out << "\n(0 rows)";
    return out.str();
}

std::string SqlExecutor::ExecuteUpdate(const UpdateStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.db_name, stmt.table_name);

    int pk_idx = schema.PrimaryKeyIndex();
    std::vector<std::pair<int, std::string>> assignments;
    for (const auto& [column, literal] : stmt.assignments) {
        int idx = schema.ColumnIndex(column);
        if (idx < 0) throw std::runtime_error("no such column: " + column);
        if (idx == pk_idx) throw std::runtime_error("cannot update the primary key column: " + column);
        if (schema.columns[idx].type == ColumnType::kInt) ValidateIntLiteral(column, literal);
        assignments.emplace_back(idx, literal);
    }

    size_t updated = 0;
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.db_name, stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
        PadRow(schema, row);
        if (!MatchesWhere(schema, row, stmt.where)) continue;
        for (const auto& [idx, literal] : assignments) row[idx] = literal;
        engine_.Put(key, EncodeRow(row));
        updated++;
    }
    return std::to_string(updated) + " row(s) updated";
}

std::string SqlExecutor::ExecuteDelete(const DeleteStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.db_name, stmt.table_name);

    size_t deleted = 0;
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.db_name, stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
        PadRow(schema, row);
        if (!MatchesWhere(schema, row, stmt.where)) continue;
        engine_.Delete(key);
        deleted++;
    }
    return std::to_string(deleted) + " row(s) deleted";
}

std::string SqlExecutor::ExecuteShowTables(const ShowTablesStatement& stmt) {
    RequireDatabaseExists(stmt.db_name);
    const std::string schema_prefix = "__schema__/" + stmt.db_name + "/";

    std::ostringstream out;
    out << "table";
    size_t count = 0;
    for (const auto& [key, blob] : engine_.Scan(schema_prefix)) {
        (void)blob;
        out << '\n' << key.substr(schema_prefix.size());
        count++;
    }
    if (count == 0) out << "\n(0 rows)";
    return out.str();
}

std::string SqlExecutor::ExecuteShowDatabases(const ShowDatabasesStatement&) {
    static const std::string kDatabasePrefix = "__database__/";

    std::ostringstream out;
    out << "database";
    size_t count = 0;
    for (const auto& [key, blob] : engine_.Scan(kDatabasePrefix)) {
        (void)blob;
        out << '\n' << key.substr(kDatabasePrefix.size());
        count++;
    }
    if (count == 0) out << "\n(0 rows)";
    return out.str();
}

}  // namespace distdb
