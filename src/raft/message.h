#pragma once

#include <cstdint>
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
struct InstallSnapshotRequest {
    Term term = 0;
    NodeId leader_id = 0;
    LogIndex last_included_index = 0;
    Term last_included_term = 0;
    std::string data;
};

struct InstallSnapshotResponse {
    Term term = 0;
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

}  // namespace distdb
