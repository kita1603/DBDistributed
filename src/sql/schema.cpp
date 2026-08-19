#include "schema.h"

#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace distdb {

namespace {

void AppendString(std::string& out, const std::string& s) {
    uint32_t len = static_cast<uint32_t>(s.size());
    out.append(reinterpret_cast<const char*>(&len), sizeof(len));
    out.append(s);
}

std::string ReadString(const std::string& blob, size_t& pos) {
    uint32_t len = 0;
    if (pos + sizeof(len) > blob.size()) throw std::runtime_error("corrupt table schema");
    std::memcpy(&len, blob.data() + pos, sizeof(len));
    pos += sizeof(len);
    if (pos + len > blob.size()) throw std::runtime_error("corrupt table schema");
    std::string s = blob.substr(pos, len);
    pos += len;
    return s;
}

uint8_t ReadByte(const std::string& blob, size_t& pos) {
    if (pos + 1 > blob.size()) throw std::runtime_error("corrupt table schema");
    return static_cast<uint8_t>(blob[pos++]);
}

}  // namespace

int TableSchema::ColumnIndex(const std::string& name) const {
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i].name == name) return static_cast<int>(i);
    }
    return -1;
}

int TableSchema::PrimaryKeyIndex() const {
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i].primary_key) return static_cast<int>(i);
    }
    return -1;
}

std::string TableSchema::Serialize() const {
    std::string out;
    uint32_t count = static_cast<uint32_t>(columns.size());
    out.append(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& col : columns) {
        AppendString(out, col.name);
        out.push_back(col.type == ColumnType::kInt ? 1 : 0);
        out.push_back(col.primary_key ? 1 : 0);
        out.push_back(col.default_value.has_value() ? 1 : 0);
        if (col.default_value) AppendString(out, *col.default_value);
    }
    return out;
}

TableSchema TableSchema::Deserialize(const std::string& blob) {
    size_t pos = 0;
    if (pos + sizeof(uint32_t) > blob.size()) throw std::runtime_error("corrupt table schema");
    uint32_t count = 0;
    std::memcpy(&count, blob.data() + pos, sizeof(count));
    pos += sizeof(count);

    TableSchema schema;
    schema.columns.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        ColumnDef col;
        col.name = ReadString(blob, pos);
        col.type = ReadByte(blob, pos) ? ColumnType::kInt : ColumnType::kText;
        col.primary_key = ReadByte(blob, pos) != 0;
        if (ReadByte(blob, pos)) col.default_value = ReadString(blob, pos);
        schema.columns.push_back(std::move(col));
    }
    if (schema.columns.empty()) throw std::runtime_error("corrupt table schema: empty column list");
    return schema;
}

}  // namespace distdb
