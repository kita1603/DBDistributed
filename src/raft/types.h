#pragma once

#include <cstdint>
#include <string>

namespace distdb {

using NodeId = uint32_t;
using Term = uint64_t;
using LogIndex = uint64_t;

// A node's network address. Lives here (rather than in node.h) so
// anything that just needs to know how to reach a node - the shard
// routing table, the standalone cross-cluster client helpers - can use
// the exact same type without pulling in all of RaftNode.
struct PeerAddress {
    std::string host;
    uint16_t port;
};

}  // namespace distdb
