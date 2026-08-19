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

// Queries a single node directly - just its bare network address, not a
// whole `cluster_peers` map - for its own id/address plus everything it
// currently knows about the rest of the cluster (see
// RaftNode::HandleDescribeCluster), and returns the combined membership
// (including that node itself) ready to hand to SendClientRequest/
// SendReadRequest. Lets a client bootstrap a full peer map from just one
// seed node's address instead of needing every member's address upfront.
// Throws std::runtime_error if the node is unreachable.
std::map<NodeId, PeerAddress> DiscoverCluster(const std::string& host, uint16_t port, int timeout_ms);

}  // namespace distdb
