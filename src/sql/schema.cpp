#include "schema.h"

#include <sstream>
#include <stdexcept>

namespace distdb {

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
    std::ostringstream oss;
    for (size_t i = 0; i < columns.size(); i++) {
        if (i > 0) oss << ',';
        oss << columns[i].name << ':' << (columns[i].type == ColumnType::kInt ? "INT" : "TEXT");
        if (columns[i].primary_key) oss << ":PK";
    }
    return oss.str();
}

TableSchema TableSchema::Deserialize(const std::string& blob) {
    TableSchema schema;
    std::istringstream stream(blob);
    std::string field;
    while (std::getline(stream, field, ',')) {
        std::istringstream field_stream(field);
        std::string name;
        std::string type_str;
        std::string pk_marker;
        std::getline(field_stream, name, ':');
        std::getline(field_stream, type_str, ':');
        bool is_pk = static_cast<bool>(std::getline(field_stream, pk_marker, ':'));

        ColumnDef col;
        col.name = name;
        col.type = (type_str == "INT") ? ColumnType::kInt : ColumnType::kText;
        col.primary_key = is_pk;
        schema.columns.push_back(col);
    }
    if (schema.columns.empty()) throw std::runtime_error("corrupt table schema: " + blob);
    return schema;
}

}  // namespace distdb
