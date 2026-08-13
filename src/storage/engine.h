#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "memtable.h"
#include "sstable.h"
#include "wal.h"

namespace distdb {

// Phase 1 single-node engine: WAL -> Memtable, and once the memtable
// grows past a threshold, flush it to an immutable SSTable on disk and
// truncate the WAL. Accumulated SSTables are periodically compacted into
// one.
//
// Get() checks the memtable first (the most recent writes), then
// SSTables from newest to oldest, stopping at the first layer that has
// an answer for the key - a live value, or a tombstone that must shadow
// whatever an older layer holds for the same key.
class StorageEngine {
 public:
    explicit StorageEngine(std::string db_path);

    // Creates db_path if needed, opens existing SSTables (newest first)
    // and the WAL, then replays the WAL to rebuild the memtable.
    void Open();

    void Put(const std::string& key, const std::string& value);
    void Delete(const std::string& key);
    std::optional<std::string> Get(const std::string& key) const;

    // Every live (non-tombstoned) key/value pair whose key starts with
    // `prefix`, merged across the memtable and every SSTable (the newest
    // layer wins per key), in ascending key order. There's no secondary
    // indexing yet, so this is how the SQL layer implements table scans -
    // it reads every row under a table's prefix regardless of any filter
    // applied afterward.
    std::vector<std::pair<std::string, std::string>> Scan(const std::string& prefix) const;

    // Wipes all durable state (WAL, every SSTable) and starts fresh, as
    // if this were a brand-new empty database. Used when installing a
    // Raft snapshot, which replaces this node's entire state wholesale
    // rather than incrementally applying commands - so whatever was here
    // before must be discarded first, not merged with.
    void Reset();

 private:
    void FlushMemtable();
    void MaybeCompact();
    std::string SSTablePath(uint64_t seq) const;

    std::string db_path_;
    WriteAheadLog wal_;
    Memtable memtable_;
    std::vector<std::unique_ptr<SSTableReader>> sstables_;  // newest first
    uint64_t next_sstable_seq_ = 1;
};

}  // namespace distdb
