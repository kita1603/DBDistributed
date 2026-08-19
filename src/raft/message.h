#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "types.h"

namespace distdb {

// A single entry in the replicated log. `command` is opaque bytes - a
// later milestone will put a serialized KV/SQL mutation here. This
// milestone (leader election + heartbeats) never actually appends real
// entries, but the wire format supports them already so log replication
// doesn't need a format change later.
struct LogEntry {
    LogIndex index = 0;
    Term term = 0;
    std::string command;
};

struct RequestVoteRequest {
    Term term = 0;
    NodeId candidate_id = 0;
    LogIndex last_log_index = 0;
    Term last_log_term = 0;
};

struct RequestVoteResponse {
    Term term = 0;
    bool vote_granted = false;
};

// Also doubles as a heartbeat when `entries` is empty.
struct AppendEntriesRequest {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex prev_log_index = 0;
    Term prev_log_term = 0;
    std::vector<LogEntry> entries;
    LogIndex leader_commit = 0;
};

struct AppendEntriesResponse {
    Term term = 0;
    bool success = false;
    LogIndex match_index = 0;  // lets the leader advance nextIndex/matchIndex without guessing
};

// Sent by a client (or, when the receiving node isn't the leader, by
// that node forwarding on the client's behalf) asking for `command` to
// be proposed to the cluster.
struct ClientRequest {
    std::string command;
};

// `leader_hint` means different things depending on `success`: on
// success it's the id of the node that actually committed the entry
// (which might not be whoever received the original request, if it had
// to forward); on failure with `not_leader` set, it's that node's best
// guess at who the real leader is, so the caller can retry there.
struct ClientResponse {
    bool success = false;
    LogIndex index = 0;
    bool not_leader = false;
    NodeId leader_hint = 0;
    std::string error;
};

// Sent by a leader to a follower whose nextIndex has fallen at or before
// the leader's own log.snapshot_index() - meaning the entries that
// follower needs have already been compacted away, so there's nothing
// left to replay via AppendEntries. `data` is an opaque (to Raft) dump of
// the whole state machine as of `last_included_index`/`last_included_term`,
// produced by the application's SnapshotCallback; the receiver hands it to
// its own RestoreCallback to adopt it wholesale.
// `membership` is the sender's current effective cluster membership,
// self-inclusive (the leader's own peers_ plus its own id/address) - unlike
// AppendEntries-driven membership updates (which a follower derives itself
// by folding ConfChange log entries), a follower installing a snapshot may
// have no usable local log to fold from at all, so it adopts this wholesale
// instead, exactly like it adopts `data` wholesale.
struct InstallSnapshotRequest {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex last_included_index = 0;
    Term last_included_term = 0;
    std::string data;
    std::map<NodeId, PeerAddress> membership;
};

struct InstallSnapshotResponse {
    Term term = 0;
};

// A read-only query, opaque to Raft (like ClientRequest's command). Reads
// don't need consensus - any node can serve one straight from its local
// state machine (see RaftNode's ReadCallback) - so unlike ClientRequest
// there's no leader check or redirect involved at all.
struct ReadRequest {
    std::string query;
};

struct ReadResponse {
    bool error = false;
    std::string result;  // the query's result on success, or an error message when `error`
};

// Lets a client discover a whole cluster's membership starting from just
// one seed node's address, instead of needing every member's address
// upfront - a client only has to know how to reach *one* node to build the
// full peer map SendClientRequest/SendReadRequest need. No consensus
// involved (like ReadRequest, any node - leader or follower - answers from
// its own current state, no leader check), so this carries no fields.
struct DescribeClusterRequest {};

// `id`/`own_address` are the *responding* node's own identity - a client
// only has a bare network address for its seed node, not which NodeId it
// is, so this is the only way to learn that. `peers` is everyone else that
// node currently knows about (its own RaftNode::peers_) - self-exclusive,
// matching that field's own convention - so the caller combines
// `{id: own_address}` with `peers` to get the complete membership,
// including the node it just asked.
struct DescribeClusterResponse {
    NodeId id = 0;
    PeerAddress own_address;
    std::map<NodeId, PeerAddress> peers;
};

enum class MessageType : uint8_t {
    kRequestVoteRequest = 1,
    kRequestVoteResponse = 2,
    kAppendEntriesRequest = 3,
    kAppendEntriesResponse = 4,
    kClientRequest = 5,
    kClientResponse = 6,
    kInstallSnapshotRequest = 7,
    kInstallSnapshotResponse = 8,
    kReadRequest = 9,
    kReadResponse = 10,
    kDescribeClusterRequest = 11,
    kDescribeClusterResponse = 12,
};

// Each Encode* function produces a type-tagged binary buffer:
// [1-byte MessageType][type-specific fields]. RaftTransport handles the
// length-prefix framing needed to send this over a TCP byte stream -
// these functions only deal with the message body.
std::string EncodeRequestVoteRequest(const RequestVoteRequest& req);
std::string EncodeRequestVoteResponse(const RequestVoteResponse& resp);
std::string EncodeAppendEntriesRequest(const AppendEntriesRequest& req);
std::string EncodeAppendEntriesResponse(const AppendEntriesResponse& resp);
std::string EncodeClientRequest(const ClientRequest& req);
std::string EncodeClientResponse(const ClientResponse& resp);
std::string EncodeInstallSnapshotRequest(const InstallSnapshotRequest& req);
std::string EncodeInstallSnapshotResponse(const InstallSnapshotResponse& resp);
std::string EncodeReadRequest(const ReadRequest& req);
std::string EncodeReadResponse(const ReadResponse& resp);
std::string EncodeDescribeClusterRequest(const DescribeClusterRequest& req);
std::string EncodeDescribeClusterResponse(const DescribeClusterResponse& resp);

// Throws std::runtime_error if `body` is too short for its declared
// fields (a corrupt or truncated message).
MessageType PeekMessageType(const std::string& body);
RequestVoteRequest DecodeRequestVoteRequest(const std::string& body);
RequestVoteResponse DecodeRequestVoteResponse(const std::string& body);
AppendEntriesRequest DecodeAppendEntriesRequest(const std::string& body);
AppendEntriesResponse DecodeAppendEntriesResponse(const std::string& body);
ClientRequest DecodeClientRequest(const std::string& body);
ClientResponse DecodeClientResponse(const std::string& body);
InstallSnapshotRequest DecodeInstallSnapshotRequest(const std::string& body);
InstallSnapshotResponse DecodeInstallSnapshotResponse(const std::string& body);
ReadRequest DecodeReadRequest(const std::string& body);
ReadResponse DecodeReadResponse(const std::string& body);
DescribeClusterRequest DecodeDescribeClusterRequest(const std::string& body);
DescribeClusterResponse DecodeDescribeClusterResponse(const std::string& body);

}  // namespace distdb
