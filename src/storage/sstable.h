#pragma once

#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "lookup.h"

namespace distdb {

// Writes an immutable SSTable. Entries must be added in ascending key
// order (the memtable's std::map already iterates that way) since the
// index block written by Finish() is a plain sorted list meant to be
// binary-searched, not sorted itself.
//
// File layout:
//   [data block]  one entry per key, in ascending key order
//   [index block] one (key, offset-into-data-block) pair per entry
//   [footer]      index_offset(8) + entry_count(4) + magic(4), fixed 16 bytes
//
// Writes go to "<path>.tmp"; Finish() only renames it to `path` once the
// entire file (data + index + footer) is flushed and closed. That rename
// is what makes a flush crash-safe: a crash mid-write leaves an orphaned
// .tmp file that Open() never mistakes for a valid table, instead of a
// half-written .sst that would be.
class SSTableWriter {
 public:
    explicit SSTableWriter(std::string path);

    void Add(const std::string& key, const std::optional<std::string>& value);
    void Finish();

 private:
    std::string path_;
    std::string tmp_path_;
    std::ofstream file_;
    std::vector<std::pair<std::string, uint64_t>> index_;
};

// Opens an SSTable produced by SSTableWriter. Open() loads its index block
// into memory so Lookup() needs only a binary search plus one seek+read
// into the data block, rather than scanning the whole file.
class SSTableReader {
 public:
    explicit SSTableReader(std::string path);

    void Open();

    LookupResult Lookup(const std::string& key) const;

    // Every entry whose key starts with `prefix`, in ascending key order,
    // found via a binary search into the in-memory index rather than a
    // full scan. Used by table scans (SELECT/UPDATE/DELETE).
    std::vector<std::pair<std::string, std::optional<std::string>>> Scan(const std::string& prefix) const;

    // Every entry in ascending key order. Used by compaction to merge
    // several SSTables together; materializes the whole table in memory,
    // which is fine at this project's scale but is exactly the step a
    // production engine would stream instead.
    std::vector<std::pair<std::string, std::optional<std::string>>> ReadAll() const;

    const std::string& path() const { return path_; }

 private:
    std::string path_;
    mutable std::ifstream file_;
    std::vector<std::pair<std::string, uint64_t>> index_;
    uint64_t data_end_ = 0;  // == the index block's start offset
};

}  // namespace distdb
