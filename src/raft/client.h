#pragma once

#include <map>
#include <string>

#include "message.h"
#include "types.h"

namespace distdb {

// Talks to a Raft cluster this process isn't itself a member of (e.g.
// a different shard), given only its peers' addresses. Sends a
// ClientRequest to one of them and, if it isn't the leader, follows its
// leader_hint (looked up in the same peer map) up to a few times - the
// same redirect-following RaftNode::ProposeOrForward does for a node
// that *is* a member, just starting from a plain network call instead
// of an in-process Propose(). Throws nothing itself; failures come back
// as a ClientResponse with success=false.
ClientResponse SendClientRequest(const std::map<NodeId, PeerAddress>& cluster_peers, const std::string& command,
                                  int timeout_ms);

// Sends a read-only query to any reachable node in `cluster_peers` and
// returns its result - tries each peer in turn until one responds, since
// (unlike a write) a read doesn't need the leader specifically, any
// replica can answer. Throws std::runtime_error if every peer is
// unreachable, or if the peer that did respond reports a query error.
std::string SendReadRequest(const std::map<NodeId, PeerAddress>& cluster_peers, const std::string& query,
                             int timeout_ms);

}  // namespace distdb
