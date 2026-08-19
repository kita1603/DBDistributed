#pragma once

#include <string>
#include <vector>

#include "ast.h"

namespace distdb {

// A table's column list, persisted in the StorageEngine under a
// "__schema__/<table>" key so it survives restarts the same way row data
// does.
struct TableSchema {
    std::vector<ColumnDef> columns;

    int ColumnIndex(const std::string& name) const;  // -1 if not found
    int PrimaryKeyIndex() const;                     // -1 if none (shouldn't happen for a valid schema)

    // Column names/type/PK-ness are plain identifiers, but a column's
    // default_value (see ColumnDef) is an arbitrary literal - same
    // arbitrary-bytes concern EncodeRow/DecodeRow (executor.cpp) exists
    // for - so this uses the same length-prefixed binary encoding rather
    // than a delimited text format.
    std::string Serialize() const;
    static TableSchema Deserialize(const std::string& blob);
};

}  // namespace distdb
