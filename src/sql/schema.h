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

    // Column names are plain identifiers (no ':' or ',' by construction,
    // since the parser only accepts identifier characters), so a simple
    // delimited text format is safe here. Row *values* need real
    // length-prefixed binary encoding instead, since they can hold
    // arbitrary bytes - see EncodeRow/DecodeRow in executor.cpp.
    std::string Serialize() const;
    static TableSchema Deserialize(const std::string& blob);
};

}  // namespace distdb
