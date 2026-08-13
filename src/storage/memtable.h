#pragma once

#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "lookup.h"

namespace distdb {

// In-memory sorted table holding the most recent writes. Backed durably
// by the WriteAheadLog, so this can always be rebuilt by replaying it -
// and cleared once its contents have been flushed to an SSTable, since
// at that point the SSTable is the durable copy instead.
//
// A deleted key is stored as a tombstone (nullopt) rather than erased.
// That distinction matters because an SSTable on disk may still hold an
// older value for the same key: the tombstone here must shadow it, so
// "absent from the map" and "explicitly deleted" cannot be conflated.
class Memtable {
 public:
    void Put(const std::string& key, const std::string& value);
    void Delete(const std::string& key);

    // kNotFound means the caller should check older layers (SSTables).
    // kDeleted means stop - a tombstone here shadows anything older.
    LookupResult Lookup(const std::string& key) const;

    const std::map<std::string, std::optional<std::string>>& Entries() const { return entries_; }
    void Clear() { entries_.clear(); }
    size_t size() const { return entries_.size(); }

    // Every entry (including tombstones) whose key starts with `prefix`,
    // in ascending key order. Used by table scans - the SQL layer has no
    // secondary indexing, so SELECT/UPDATE/DELETE walk every row under a
    // table's key prefix rather than doing a point lookup.
    std::vector<std::pair<std::string, std::optional<std::string>>> Scan(const std::string& prefix) const;

 private:
    std::map<std::string, std::optional<std::string>> entries_;
};

}  // namespace distdb
