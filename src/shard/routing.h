#pragma once

#include <map>
#include <string>
#include <vector>

#include "../raft/types.h"

namespace distdb {

using ShardId = uint32_t;

// One contiguous slice of the whole key space: [start_key, end_key), with
// its own independent Raft group (peers). An empty start_key means
// "-infinity" (matches everything from the very first key); an empty
// end_key means "+infinity". Ranges in a well-formed routing table are
// contiguous and non-overlapping, covering the entire key space.
struct ShardRange {
    ShardId id = 0;
    std::string start_key;
    std::string end_key;
    std::map<NodeId, PeerAddress> peers;
};

// A static, hand-configured map from key ranges to the shard (and its
// Raft group's peer addresses) responsible for them. Loaded once from a
// text file at startup and never changes at runtime - splitting/merging/
// rebalancing shards would need this to become itself replicated across
// the cluster instead of a local file, which is out of scope here (see
// the README).
//
// File format, one shard per line:
//   <shard_id> <start_key> <end_key> <peers>
// `*` for start_key/end_key means unbounded on that side (real keys in
// this project always start with "__row__"/"__schema__", so they can
// never literally equal "*" - safe to use as a sentinel with no
// escaping). `peers` reuses the same "id=host:port,id=host:port" syntax
// already used for a single Raft group's peer list. Lines that are
// blank or start with '#' are ignored.
class RoutingTable {
 public:
    void LoadFromFile(const std::string& path);

    // Binary search over the sorted range boundaries - the same "sorted
    // array + binary search" pattern SSTableReader uses to find which
    // block holds a key, just one level up: here it's which shard holds
    // a key, instead of which offset within one file.
    const ShardRange& ShardFor(const std::string& key) const;

    const ShardRange& Shard(ShardId id) const;
    std::vector<ShardId> AllShardIds() const;

 private:
    std::vector<ShardRange> shards_;  // sorted by start_key
};

}  // namespace distdb
