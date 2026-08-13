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

}  // namespace

SqlExecutor::SqlExecutor(StorageEngine& engine) : engine_(engine) {}

std::string SqlExecutor::SchemaKey(const std::string& table_name) { return "__schema__/" + table_name; }
std::string SqlExecutor::RowPrefix(const std::string& table_name) { return "__row__/" + table_name + "/"; }
std::string SqlExecutor::RowKey(const std::string& table_name, const std::string& pk_value) {
    return RowPrefix(table_name) + pk_value;
}

TableSchema SqlExecutor::LoadSchema(const std::string& table_name) {
    auto blob = engine_.Get(SchemaKey(table_name));
    if (!blob) throw std::runtime_error("no such table: " + table_name);
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
            if constexpr (std::is_same_v<T, CreateTableStatement>) {
                return ExecuteCreateTable(s);
            } else if constexpr (std::is_same_v<T, InsertStatement>) {
                return ExecuteInsert(s);
            } else if constexpr (std::is_same_v<T, SelectStatement>) {
                return ExecuteSelect(s);
            } else if constexpr (std::is_same_v<T, UpdateStatement>) {
                return ExecuteUpdate(s);
            } else {
                return ExecuteDelete(s);
            }
        },
        stmt);
}

std::string SqlExecutor::ExecuteCreateTable(const CreateTableStatement& stmt) {
    if (engine_.Get(SchemaKey(stmt.table_name))) {
        throw std::runtime_error("table already exists: " + stmt.table_name);
    }
    TableSchema schema;
    schema.columns = stmt.columns;
    engine_.Put(SchemaKey(stmt.table_name), schema.Serialize());
    return "OK";
}

std::string SqlExecutor::ExecuteInsert(const InsertStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.table_name);

    if (stmt.columns.size() != schema.columns.size()) {
        throw std::runtime_error("INSERT must specify all " + std::to_string(schema.columns.size()) +
                                  " column(s) of table '" + stmt.table_name + "'");
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
    std::string key = RowKey(stmt.table_name, row[pk_idx]);
    if (engine_.Get(key)) throw std::runtime_error("duplicate primary key: " + row[pk_idx]);

    engine_.Put(key, EncodeRow(row));
    return "OK";
}

std::string SqlExecutor::ExecuteSelect(const SelectStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.table_name);

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
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
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
    TableSchema schema = LoadSchema(stmt.table_name);

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
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
        if (!MatchesWhere(schema, row, stmt.where)) continue;
        for (const auto& [idx, literal] : assignments) row[idx] = literal;
        engine_.Put(key, EncodeRow(row));
        updated++;
    }
    return std::to_string(updated) + " row(s) updated";
}

std::string SqlExecutor::ExecuteDelete(const DeleteStatement& stmt) {
    TableSchema schema = LoadSchema(stmt.table_name);

    size_t deleted = 0;
    for (const auto& [key, blob] : engine_.Scan(RowPrefix(stmt.table_name))) {
        std::vector<std::string> row = DecodeRow(blob);
        if (!MatchesWhere(schema, row, stmt.where)) continue;
        engine_.Delete(key);
        deleted++;
    }
    return std::to_string(deleted) + " row(s) deleted";
}

}  // namespace distdb
