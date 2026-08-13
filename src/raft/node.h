#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <map>
#include <mutex>
#include <string>

#include "log.h"
#include "message.h"
#include "persistent_state.h"
#include "transport.h"
#include "types.h"

namespace distdb {

struct PeerAddress {
    std::string host;
    uint16_t port;
};

enum class Role {
    kFollower,
    kCandidate,
    kLeader,
};

// Invoked once, in index order, for each log entry as it becomes
// committed - on the leader as soon as a majority acknowledges it, and
// on followers as they learn the leader's commit index has passed it.
// Every node ends up invoking this with the exact same sequence of
// commands in the same order, which is the whole point of a replicated
// log: it's what a storage layer would hook into to apply writes
// identically on every replica.
//
// Must not throw: it runs inside ApplyCommitted() while mutex_ is held,
// often from a detached RPC-response thread - an exception escaping a
// detached thread's top-level function calls std::terminate() and kills
// the whole process. A callback that can fail (e.g. executing SQL that
// might error) must catch internally and just log/ignore the failure.
using ApplyCallback = std::function<void(LogIndex index, const std::string& command)>;

// A from-scratch Raft node: leader election, heartbeats, and now real
// log replication. A client calls Propose() on whichever node is
// currently the leader; the command is appended to that node's log,
// replicated to followers via AppendEntries (with the standard
// prevLogIndex/prevLogTerm consistency check and conflict-truncation),
// and handed to ApplyCallback on every node once a majority holds it
// durably.
//
// Concurrency model: a single mutex_ guards all mutable state (role,
// term, log, commit index, per-follower replication progress). RPC
// handlers, the ticker, and Propose() take it for their critical section
// but release it before any network I/O, so one slow/unreachable peer
// never blocks the rest of the node.
class RaftNode {
 public:
    RaftNode(NodeId id, uint16_t listen_port, std::map<NodeId, PeerAddress> peers, std::string state_dir,
             ApplyCallback apply_callback = [](LogIndex, const std::string&) {});

    // Starts the RPC listener and the background election/heartbeat/
    // replication ticker (each on its own thread) and returns - this
    // node keeps running until the process is killed.
    void Run();

    Role role() const;
    Term current_term() const;
    NodeId leader_hint() const;  // 0 if unknown

    // Only succeeds if this node is currently the leader: appends
    // `command` to the log, triggers immediate replication, and blocks
    // until either a majority has durably replicated it (returns true,
    // *out_index set to its log index) or `timeout_ms` elapses without
    // that happening (returns false - the write's fate is genuinely
    // unconfirmed at that point, same as a real Raft client would see
    // it; the caller should retry, possibly against a different node
    // that has since become leader).
    bool Propose(const std::string& command, LogIndex* out_index, int timeout_ms = 2000);

    // Like Propose(), but works no matter which node it's called on: if
    // this node isn't the leader, it forwards the request over the
    // network to whichever node it believes is (following up to a
    // handful of redirects if that guess turns out to be stale too),
    // and returns whatever that node's Propose() call decided. This is
    // what lets a client - or this project's own REPL - send a write to
    // any node without first having to find the leader itself.
    ClientResponse ProposeOrForward(const std::string& command, int timeout_ms = 3000);

 private:
    std::string HandleMessage(const std::string& request_body);
    RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);
    ClientResponse HandleClientRequest(const ClientRequest& req);

    void TickerLoop();
    void StartElection();
    void ReplicateToAll();
    void ReplicateTo(NodeId peer_id, const PeerAddress& addr);

    // All of the below assume the caller already holds mutex_.
    void ApplyCommitted();
    void BecomeFollower(Term term);
    void BecomeLeader();
    void ResetElectionDeadline();

    NodeId id_;
    uint16_t listen_port_;
    std::map<NodeId, PeerAddress> peers_;
    ApplyCallback apply_callback_;

    mutable std::mutex mutex_;
    std::condition_variable commit_cv_;
    PersistentState state_;
    RaftLog log_;
    Role role_ = Role::kFollower;
    NodeId leader_hint_ = 0;
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;

    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point next_heartbeat_time_;
    int votes_received_ = 0;

    // Leader-only; reset whenever this node becomes leader.
    std::map<NodeId, LogIndex> next_index_;
    std::map<NodeId, LogIndex> match_index_;

    RaftTransport transport_;
};

}  // namespace distdb
