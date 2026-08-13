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
// might error) must catch internally and report the failure through the
// return value instead: empty string means the command applied
// successfully, non-empty is an error message. This is what lets
// Propose() tell a duplicate-key (or other apply-time) failure apart
// from a genuinely successful write - both commit identically, since
// commit only means "a majority durably logged this command", not
// "running it succeeded".
using ApplyCallback = std::function<std::string(LogIndex index, const std::string& command)>;

// Called when this node needs to send its current state to a badly-
// lagging follower (one whose nextIndex has fallen at or before what's
// been compacted away): must return a self-contained dump of the whole
// state machine's current contents, opaque to Raft. Same "must not
// throw" contract as ApplyCallback. Unlike ApplyCallback, this one *is*
// called with mutex_ held (see SendInstallSnapshot) - deliberately: the
// index this dump gets labeled with must be exactly what it reflects, and
// the only way to guarantee that is to make the dump and the "how far has
// this node applied" bookkeeping atomic with each other, since both
// ApplyCallback and this run under the same lock. Concretely, this can
// block other RPCs for as long as the dump takes - acceptable at this
// project's scale (the whole point is correctness: an understated index
// would let already-applied entries get re-applied later, and replaying a
// non-idempotent SQL statement like INSERT on data that's already there
// fails outright rather than silently doing nothing).
using SnapshotCallback = std::function<std::string()>;

// The receiving side of SnapshotCallback's output: replace the state
// machine's entire current content with what's encoded in `data`,
// discarding whatever was there before. Same "must not throw" contract as
// ApplyCallback, but - unlike SnapshotCallback - this one runs *without*
// mutex_ held (see HandleInstallSnapshot), so a slow restore doesn't block
// this node's ability to handle other RPCs (elections, AppendEntries from
// the same leader) meanwhile. A dedicated install_mutex_ (not mutex_)
// still guarantees only one restore runs at a time.
using RestoreCallback = std::function<void(const std::string& data)>;

// Serves a read-only query straight from local state, no consensus
// involved - any node can answer one, not just the leader (consistent
// with this project's reads-aren't-linearizable stance elsewhere).
// Should throw on a query error (e.g. bad syntax); HandleReadRequest
// catches it and reports it back as ReadResponse::error rather than
// letting it escape.
using ReadCallback = std::function<std::string(const std::string& query)>;

// A from-scratch Raft node: leader election, heartbeats, real log
// replication, and log compaction. A client calls Propose() on whichever
// node is currently the leader; the command is appended to that node's
// log, replicated to followers via AppendEntries (with the standard
// prevLogIndex/prevLogTerm consistency check and conflict-truncation),
// and handed to ApplyCallback on every node once a majority holds it
// durably. Once enough entries have been applied, the log entries behind
// them are discarded via RaftLog::CompactTo() - they're already reflected
// in the state machine, which persists itself independently, so nothing
// is lost. A follower whose needed entries have already been compacted
// away on the leader gets caught up via InstallSnapshot instead of
// AppendEntries: the leader calls SnapshotCallback for a fresh dump of
// its current state and ships that wholesale; the follower's
// RestoreCallback replaces its own state machine with it and adopts the
// same compaction boundary.
//
// Concurrency model: a single mutex_ guards all mutable state (role,
// term, log, commit index, per-follower replication progress). RPC
// handlers, the ticker, and Propose() take it for their critical section
// but release it before any network I/O - or before calling
// SnapshotCallback/RestoreCallback, which can also be slow - so one
// slow/unreachable peer, or one slow snapshot, never blocks the rest of
// the node.
class RaftNode {
 public:
    RaftNode(NodeId id, uint16_t listen_port, std::map<NodeId, PeerAddress> peers, std::string state_dir,
             ApplyCallback apply_callback = [](LogIndex, const std::string&) { return std::string(); },
             SnapshotCallback snapshot_callback = [] { return std::string(); },
             RestoreCallback restore_callback = [](const std::string&) {},
             ReadCallback read_callback = [](const std::string&) { return std::string(); });

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
    //
    // Commit and apply are different guarantees: a command can commit
    // (a majority durably logged it) and still fail when it actually
    // runs (e.g. a duplicate primary key) - the log doesn't know or
    // care what the command means. When that happens, Propose() still
    // returns true (the write IS durably committed - every replica
    // agrees on having tried it, which is what makes it safe to also
    // fail identically everywhere), but sets *out_apply_error to the
    // failure message so the caller can tell "committed and applied"
    // apart from "committed but rejected at apply time".
    bool Propose(const std::string& command, LogIndex* out_index, int timeout_ms = 2000,
                 std::string* out_apply_error = nullptr);

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
    InstallSnapshotResponse HandleInstallSnapshot(const InstallSnapshotRequest& req);
    ReadResponse HandleReadRequest(const ReadRequest& req);

    void TickerLoop();
    void StartElection();
    void ReplicateToAll();
    void ReplicateTo(NodeId peer_id, const PeerAddress& addr);
    void SendInstallSnapshot(NodeId peer_id, const PeerAddress& addr, Term term);

    // All of the below assume the caller already holds mutex_.
    void MaybeAdvanceCommitIndex();
    void ApplyCommitted();
    void BecomeFollower(Term term);
    void BecomeLeader();
    void ResetElectionDeadline();

    NodeId id_;
    uint16_t listen_port_;
    std::map<NodeId, PeerAddress> peers_;
    ApplyCallback apply_callback_;
    SnapshotCallback snapshot_callback_;
    RestoreCallback restore_callback_;
    ReadCallback read_callback_;

    mutable std::mutex mutex_;
    // Serializes HandleInstallSnapshot calls against each other (but not
    // against anything else): the accept loop spawns a thread per
    // connection, so if a leader's retries overlap - one still in flight
    // when the next round starts - two InstallSnapshot RPCs could
    // otherwise run concurrently on this follower and finish out of
    // order, letting a stale (smaller) snapshot's wholesale restore
    // clobber a newer one that already landed. This isn't mutex_ itself
    // because a slow restore_callback_ shouldn't block unrelated RPCs
    // (elections, AppendEntries) - only a second concurrent install.
    std::mutex install_mutex_;
    std::condition_variable commit_cv_;
    PersistentState state_;
    RaftLog log_;
    Role role_ = Role::kFollower;
    NodeId leader_hint_ = 0;
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;

    // Non-empty entries are indices whose apply_callback_ call returned
    // an error (e.g. duplicate key) - looked up by Propose() right after
    // commit_cv_ wakes, so it can tell a client "committed but rejected
    // at apply time" apart from a clean success. Pruned alongside log
    // compaction so this can't grow without bound on a long-running node.
    std::map<LogIndex, std::string> apply_errors_;

    std::chrono::steady_clock::time_point election_deadline_;
    std::chrono::steady_clock::time_point next_heartbeat_time_;
    int votes_received_ = 0;

    // Leader-only; reset whenever this node becomes leader.
    std::map<NodeId, LogIndex> next_index_;
    std::map<NodeId, LogIndex> match_index_;

    RaftTransport transport_;
};

}  // namespace distdb
