#include "node.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <thread>

#include "conf_change.h"

namespace distdb {

namespace {

// Standard Raft magic numbers, scaled down for fast local testing.
// Heartbeats must land well inside the election timeout window, and the
// timeout itself is randomized per node so a single election usually
// resolves without every follower timing out and splitting the vote at
// once.
constexpr auto kTickInterval = std::chrono::milliseconds(20);
constexpr auto kHeartbeatInterval = std::chrono::milliseconds(75);
constexpr int kElectionTimeoutMinMs = 400;
constexpr int kElectionTimeoutMaxMs = 600;
constexpr int kRpcTimeoutMs = 150;
constexpr int kMaxForwardHops = 3;
// A snapshot dump can be much larger than a normal heartbeat/AppendEntries
// payload, so it gets a longer RPC timeout.
constexpr int kSnapshotRpcTimeoutMs = 5000;
// Kept deliberately small (applied entries, not raw command bytes) so
// compaction is easy to trigger and observe by hand while testing. A
// real deployment would size this off log *bytes*, not entry count.
constexpr LogIndex kCompactionThreshold = 5;

int RandomElectionTimeoutMs() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(kElectionTimeoutMinMs, kElectionTimeoutMaxMs);
    return dist(rng);
}

}  // namespace

RaftNode::RaftNode(NodeId id, uint16_t listen_port, PeerAddress own_address, std::map<NodeId, PeerAddress> peers,
                    std::string state_dir, bool joining, ApplyCallback apply_callback,
                    SnapshotCallback snapshot_callback, RestoreCallback restore_callback, ReadCallback read_callback)
    : id_(id),
      listen_port_(listen_port),
      own_address_(std::move(own_address)),
      peers_(std::move(peers)),
      apply_callback_(std::move(apply_callback)),
      snapshot_callback_(std::move(snapshot_callback)),
      restore_callback_(std::move(restore_callback)),
      read_callback_(std::move(read_callback)),
      state_(state_dir + "/raft_state_" + std::to_string(id) + ".txt"),
      log_(state_dir + "/raft_log_" + std::to_string(id) + ".bin"),
      membership_state_(MembershipState::PathFor(state_dir, id)),
      joining_(joining),
      transport_(listen_port) {
    state_.Load();
    log_.Load();
    membership_state_.Load();

    if (membership_state_.HasFile()) {
        // Not this node's first-ever run: membership_state_ is
        // authoritative over whatever the caller's `peers`/`own_address`/
        // `joining` args happen to be (typically routing.conf, which is
        // only a bootstrap seed good for the very first run - it's never
        // updated after a membership change, and doesn't even list a node
        // added later via add-server at all).
        peers_ = membership_state_.base_peers();
        self_is_member_ = membership_state_.self_is_member();
        own_address_ = membership_state_.own_address();
        joining_ = false;
    } else {
        // First-ever run: seed membership_state_ from the constructor's
        // args and persist immediately, so a crash right after this
        // doesn't lose the seed.
        self_is_member_ = !joining_;
        membership_state_.Set(peers_, self_is_member_, own_address_);
    }

    // commit_index_/last_applied_ default to 0 and are never persisted
    // directly - log_.applied_index() is the durable record of how far
    // apply_callback_ has actually run (falling back to snapshot_index()
    // for a log file written before that field existed). Without this a
    // restarted node forgets its applied boundary and reports 0, which is
    // exactly what SendInstallSnapshot declares to a follower as
    // `last_included_index` - reporting 0 there makes a real, non-empty
    // snapshot look like a no-op to any follower already at or past index
    // 0 (i.e. every follower), so it's silently skipped instead of
    // installed.
    last_applied_ = log_.applied_index();
    commit_index_ = log_.applied_index();
}

Role RaftNode::role() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return role_;
}

Term RaftNode::current_term() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_.current_term();
}

NodeId RaftNode::leader_hint() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return leader_hint_;
}

void RaftNode::Run() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ResetElectionDeadline();
    }
    transport_.Serve([this](const std::string& body) { return HandleMessage(body); });
    std::thread([this] { TickerLoop(); }).detach();
}

void RaftNode::ResetElectionDeadline() {
    election_deadline_ = std::chrono::steady_clock::now() + std::chrono::milliseconds(RandomElectionTimeoutMs());
}

std::string RaftNode::HandleMessage(const std::string& request_body) {
    switch (PeekMessageType(request_body)) {
        case MessageType::kRequestVoteRequest:
            return EncodeRequestVoteResponse(HandleRequestVote(DecodeRequestVoteRequest(request_body)));
        case MessageType::kAppendEntriesRequest:
            return EncodeAppendEntriesResponse(HandleAppendEntries(DecodeAppendEntriesRequest(request_body)));
        case MessageType::kClientRequest:
            return EncodeClientResponse(HandleClientRequest(DecodeClientRequest(request_body)));
        case MessageType::kInstallSnapshotRequest:
            return EncodeInstallSnapshotResponse(HandleInstallSnapshot(DecodeInstallSnapshotRequest(request_body)));
        case MessageType::kReadRequest:
            return EncodeReadResponse(HandleReadRequest(DecodeReadRequest(request_body)));
        case MessageType::kDescribeClusterRequest:
            return EncodeDescribeClusterResponse(HandleDescribeCluster(DecodeDescribeClusterRequest(request_body)));
        default:
            throw std::runtime_error("unexpected message type received");
    }
}

void RaftNode::BecomeFollower(Term term) {
    if (term > state_.current_term()) {
        state_.Set(term, std::nullopt);
    }
    if (role_ != Role::kFollower) {
        std::cout << "[node " << id_ << "] term " << state_.current_term() << ": becoming follower\n";
    }
    role_ = Role::kFollower;
    ResetElectionDeadline();
}

void RaftNode::BecomeLeader() {
    RefreshMembership();  // defensive - make sure peers_ reflects the log before resetting per-peer state below
    role_ = Role::kLeader;
    leader_hint_ = id_;
    next_heartbeat_time_ = std::chrono::steady_clock::now();  // fire the first replication round on the next tick
    replication_in_flight_.clear();
    for (const auto& [peer_id, addr] : peers_) {
        (void)addr;
        next_index_[peer_id] = log_.LastIndex() + 1;
        match_index_[peer_id] = 0;
    }
    std::cout << "[node " << id_ << "] term " << state_.current_term() << ": *** becoming leader *** (log has "
              << log_.LastIndex() << " entries)\n";
}

void RaftNode::ApplyCommitted() {
    while (last_applied_ < commit_index_) {
        last_applied_++;
        auto entry = log_.At(last_applied_);
        // A ConfChangeCommand is pure Raft-internal plumbing, already
        // fully handled by RefreshMembership() at append time - it has
        // nothing to do with the SQL/txn state machine and must never
        // reach apply_callback_ (which knows nothing about tag byte 0x01).
        if (entry && !TryDecodeConfChange(entry->command)) {
            std::string error = apply_callback_(entry->index, entry->command);
            if (!error.empty()) apply_errors_[entry->index] = std::move(error);
        }
        // Persisted right after this entry's effect lands, not batched
        // until the loop ends: if the process crashes partway through a
        // multi-entry commit, log_.applied_index() must never claim more
        // than what apply_callback_ actually finished, or a restart would
        // skip re-running an entry whose effect never actually happened.
        log_.SetAppliedIndex(last_applied_);
    }
    commit_cv_.notify_all();

    // Everything up to last_applied_ is already durably reflected in the
    // state machine (it persists itself independently - see
    // SnapshotCallback's doc comment), so the log entries that produced
    // it can be discarded. This is a pure RaftLog operation (no
    // SnapshotCallback involved): trimming our own already-applied
    // history is always safe, unlike restoring a follower from a
    // leader's snapshot, which is the only case that needs the actual
    // state-machine dump.
    if (last_applied_ - log_.snapshot_index() >= kCompactionThreshold) {
        LogIndex old_snapshot_index = log_.snapshot_index();

        // Fold any ConfChangeCommands about to be compacted away into
        // membership_state_'s persisted base first - once CompactTo()
        // runs, log_.At() can never see them again, so whatever they
        // changed about membership would otherwise be forgotten across a
        // restart (same reason log_.applied_index() needs its own
        // persisted field distinct from snapshot_index_). Every entry in
        // this range is already committed (ApplyCommitted only reaches
        // entries up to commit_index_), so unlike RefreshMembership()
        // there's no append-vs-commit timing subtlety to worry about here.
        std::map<NodeId, PeerAddress> new_base = membership_state_.base_peers();
        bool new_base_self_member = membership_state_.self_is_member();
        bool membership_changed = false;
        for (LogIndex i = old_snapshot_index + 1; i <= last_applied_; i++) {
            auto entry = log_.At(i);
            if (!entry) continue;
            auto cc = TryDecodeConfChange(entry->command);
            if (!cc) continue;
            membership_changed = true;
            if (cc->type == ConfChangeType::kAddServer) {
                if (cc->server_id == id_) {
                    new_base_self_member = true;
                } else {
                    new_base[cc->server_id] = {cc->host, cc->port};
                }
            } else {
                if (cc->server_id == id_) {
                    new_base_self_member = false;
                } else {
                    new_base.erase(cc->server_id);
                }
            }
        }
        if (membership_changed) membership_state_.Set(std::move(new_base), new_base_self_member, own_address_);

        log_.CompactTo(last_applied_, log_.TermAt(last_applied_));
        // Prune up to the *previous* boundary, not the new one: the
        // entries between them (including whatever was just applied
        // above, in this very call) may still be waiting on a Propose()
        // that hasn't checked apply_errors_ yet - erasing up to the new
        // boundary would delete a just-recorded error before its own
        // Propose() call ever got to read it.
        apply_errors_.erase(apply_errors_.begin(), apply_errors_.upper_bound(old_snapshot_index));
        std::cout << "[node " << id_ << "] compacted log: snapshot index " << old_snapshot_index << " -> "
                  << log_.snapshot_index() << "\n";
    }
}

void RaftNode::RefreshMembership() {
    std::map<NodeId, PeerAddress> effective = membership_state_.base_peers();
    bool effective_self_member = membership_state_.self_is_member();
    pending_self_removal_index_ = 0;

    for (LogIndex i = log_.snapshot_index() + 1; i <= log_.LastIndex(); i++) {
        auto entry = log_.At(i);
        if (!entry) continue;
        auto cc = TryDecodeConfChange(entry->command);
        if (!cc) continue;

        if (cc->type == ConfChangeType::kAddServer) {
            if (cc->server_id == id_) {
                effective_self_member = true;
                pending_self_removal_index_ = 0;  // supersedes any earlier pending self-removal
            } else {
                effective[cc->server_id] = {cc->host, cc->port};
            }
        } else {  // kRemoveServer
            if (cc->server_id == id_) {
                if (role_ == Role::kLeader && commit_index_ < i) {
                    // Leader removing itself can't act on it until this
                    // entry actually commits - see this method's doc
                    // comment in node.h for why.
                    pending_self_removal_index_ = i;
                } else {
                    // Either not leader (no obligation to keep acting as
                    // a member past append time), or already committed -
                    // safe to reflect immediately.
                    effective_self_member = false;
                }
            } else {
                effective.erase(cc->server_id);
            }
        }
    }

    self_is_member_ = effective_self_member;

    for (const auto& [pid, addr] : effective) {
        bool is_new = !peers_.count(pid);
        peers_[pid] = addr;
        if (is_new && role_ == Role::kLeader) {
            // A brand-new peer has zero state, so seed low - this lets
            // ReplicateTo's needs_snapshot check pick the right catch-up
            // path (full log replay or InstallSnapshot) on the very first
            // attempt, instead of linearly backing off one index per
            // heartbeat from a guess that's certainly too high.
            next_index_[pid] = 1;
            match_index_[pid] = 0;
        }
    }
    for (auto it = peers_.begin(); it != peers_.end();) {
        if (!effective.count(it->first)) {
            next_index_.erase(it->first);
            match_index_.erase(it->first);
            replication_in_flight_.erase(it->first);
            it = peers_.erase(it);
        } else {
            ++it;
        }
    }
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);

    // Raft thesis section 9.6's mitigation for disruptive servers: if a
    // real leader has been in contact recently, ignore this vote request
    // entirely (not even a term comparison) - see last_leader_contact_'s
    // doc comment in node.h for why this matters once servers can be
    // removed.
    if (std::chrono::steady_clock::now() - last_leader_contact_ < std::chrono::milliseconds(kElectionTimeoutMinMs)) {
        return {state_.current_term(), false};
    }

    if (req.term < state_.current_term()) {
        return {state_.current_term(), false};
    }
    if (req.term > state_.current_term()) {
        BecomeFollower(req.term);
    }

    auto voted_for = state_.voted_for();
    bool already_voted_elsewhere = voted_for.has_value() && *voted_for != req.candidate_id;

    // Raft grants a vote only if the candidate's log is at least as
    // up-to-date as ours (compare last entry's term first, then index),
    // so a candidate that's missing committed entries can never win.
    Term my_last_term = log_.LastTerm();
    LogIndex my_last_index = log_.LastIndex();
    bool candidate_log_ok = (req.last_log_term > my_last_term) ||
                             (req.last_log_term == my_last_term && req.last_log_index >= my_last_index);

    if (!already_voted_elsewhere && candidate_log_ok) {
        state_.Set(state_.current_term(), req.candidate_id);
        ResetElectionDeadline();
        std::cout << "[node " << id_ << "] term " << state_.current_term() << ": voted for node "
                  << req.candidate_id << "\n";
        return {state_.current_term(), true};
    }
    return {state_.current_term(), false};
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);

    if (req.term < state_.current_term()) {
        return {state_.current_term(), false, 0};
    }
    if (req.term > state_.current_term() || role_ != Role::kFollower) {
        BecomeFollower(req.term);
    }
    ResetElectionDeadline();
    last_leader_contact_ = std::chrono::steady_clock::now();
    leader_hint_ = req.leader_id;

    bool ok = log_.AppendEntriesFrom(req.prev_log_index, req.prev_log_term, req.entries);
    if (!ok) {
        return {state_.current_term(), false, 0};
    }
    RefreshMembership();

    if (req.leader_commit > commit_index_) {
        commit_index_ = std::min(req.leader_commit, log_.LastIndex());
        ApplyCommitted();
    }

    LogIndex match_index = req.prev_log_index + req.entries.size();
    return {state_.current_term(), true, match_index};
}

void RaftNode::TickerLoop() {
    while (true) {
        std::this_thread::sleep_for(kTickInterval);

        bool start_election = false;
        bool replicate = false;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto now = std::chrono::steady_clock::now();
            if (role_ != Role::kLeader && now >= election_deadline_) {
                start_election = true;
            } else if (role_ == Role::kLeader && now >= next_heartbeat_time_) {
                replicate = true;
                next_heartbeat_time_ = now + kHeartbeatInterval;
            }
        }

        if (start_election) {
            StartElection();
        } else if (replicate) {
            ReplicateToAll();
        }
    }
}

void RaftNode::StartElection() {
    Term election_term;
    LogIndex last_index;
    Term last_term;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        RefreshMembership();
        if (!self_is_member_) {
            // Not (or no longer, or not yet) a member of this cluster -
            // removed nodes and nodes still --joining must never
            // self-elect. Reset the deadline so TickerLoop doesn't just
            // call back in here every single tick.
            ResetElectionDeadline();
            return;
        }
        state_.Set(state_.current_term() + 1, id_);
        role_ = Role::kCandidate;
        votes_received_ = 1;  // votes for itself
        election_term = state_.current_term();
        last_index = log_.LastIndex();
        last_term = log_.LastTerm();
        ResetElectionDeadline();
        std::cout << "[node " << id_ << "] term " << election_term << ": starting election\n";

        // A degenerate but valid case: a single-node cluster (peers_
        // empty) already has a majority from its own vote alone. The
        // usual path to BecomeLeader() only runs as a side effect of
        // processing a peer's RequestVote response below - with no
        // peers, that loop body never executes, so without this check
        // a lone node would re-run elections forever without ever
        // noticing it already won.
        if (votes_received_ * 2 > static_cast<int>(peers_.size() + 1)) {
            BecomeLeader();
            return;
        }
    }

    std::string encoded = EncodeRequestVoteRequest({election_term, id_, last_index, last_term});

    for (const auto& [peer_id, addr] : peers_) {
        (void)peer_id;
        std::thread([this, addr, encoded, election_term] {
            std::string response;
            try {
                response = RaftTransport::SendRequest(addr.host, addr.port, encoded, kRpcTimeoutMs);
            } catch (...) {
                return;  // peer unreachable right now - just doesn't get counted
            }
            RequestVoteResponse resp = DecodeRequestVoteResponse(response);

            std::lock_guard<std::mutex> lock(mutex_);
            if (resp.term > state_.current_term()) {
                BecomeFollower(resp.term);
                return;
            }
            if (role_ != Role::kCandidate || election_term != state_.current_term()) {
                return;  // stale response from an election we've already left
            }
            if (resp.vote_granted) {
                votes_received_++;
                if (votes_received_ * 2 > static_cast<int>(peers_.size() + 1)) {
                    BecomeLeader();
                }
            }
        }).detach();
    }
}

void RaftNode::ReplicateToAll() {
    std::map<NodeId, PeerAddress> peers_copy;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader) return;
        peers_copy = peers_;
    }
    for (const auto& [peer_id, addr] : peers_copy) {
        ReplicateTo(peer_id, addr);
    }
}

// Advance commit_index_ to the highest index a majority (including this
// node itself) has acknowledged - but only count it if that entry is
// from our own current term. Committing an older-term entry directly
// (rather than it committing as a side effect of a later same-term
// entry committing) is exactly the unsafe case Raft's Figure 8 warns
// about. Caller must hold mutex_. With zero peers (a single-node
// cluster), match_indices ends up holding just the leader's own
// log_.LastIndex() - the leader alone is already a majority of one,
// so this still correctly advances commit_index_ without waiting on
// any peer response.
void RaftNode::MaybeAdvanceCommitIndex() {
    std::vector<LogIndex> match_indices;
    match_indices.push_back(log_.LastIndex());  // the leader always has everything it's logged
    for (const auto& [pid, matched] : match_index_) {
        (void)pid;
        match_indices.push_back(matched);
    }
    std::sort(match_indices.begin(), match_indices.end(), std::greater<LogIndex>());
    LogIndex majority_index = match_indices[match_indices.size() / 2];

    if (majority_index > commit_index_ && log_.TermAt(majority_index) == state_.current_term()) {
        commit_index_ = majority_index;
        ApplyCommitted();

        // A leader that proposed its own removal must not stop leading
        // until that removal actually commits - see RefreshMembership()'s
        // doc comment for why stepping down at append time instead could
        // strand the entry below majority forever. Once commit_index_
        // catches up, re-derive membership so self_is_member_ flips
        // immediately, rather than waiting for the next unrelated
        // Propose()/AppendEntries call to happen to notice.
        if (pending_self_removal_index_ != 0 && commit_index_ >= pending_self_removal_index_) {
            bool was_leader = role_ == Role::kLeader;
            RefreshMembership();
            if (was_leader && !self_is_member_) {
                role_ = Role::kFollower;
                std::cout << "[node " << id_ << "] removed from the cluster - stepping down\n";
            }
        }
    }
}

// Sends at most one outstanding RPC to `peer_id` at a time. Without this,
// TickerLoop's every-75ms heartbeat would spawn a brand new detached
// thread/connection to the peer on every tick regardless of whether an
// earlier attempt (possibly still waiting out its own timeout - up to 5s
// for InstallSnapshot) had finished. Under sustained failure that grows
// unboundedly: dozens of concurrent connection attempts pile up against
// the same peer, and in practice that can choke the peer badly enough
// that *no* RPC to it - including its own outbound RequestVotes - ever
// gets a chance to complete. replication_in_flight_ is cleared as soon as
// this peer's one outstanding attempt (of either kind) resolves, by every
// exit path in the two response-handling threads below.
void RaftNode::ReplicateTo(NodeId peer_id, const PeerAddress& addr) {
    Term term;
    LogIndex prev_log_index;
    Term prev_log_term;
    std::vector<LogEntry> entries_to_send;
    LogIndex leader_commit;
    bool needs_snapshot = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader) return;
        if (replication_in_flight_.count(peer_id)) return;  // still waiting on a prior RPC to this peer
        term = state_.current_term();
        LogIndex next_idx = next_index_[peer_id];
        if (next_idx <= log_.snapshot_index()) {
            // The entries this follower needs have already been
            // compacted away on our side - there's nothing left to
            // replay via AppendEntries, so it needs our current state
            // wholesale instead.
            needs_snapshot = true;
        } else {
            prev_log_index = next_idx - 1;
            prev_log_term = log_.TermAt(prev_log_index);
            for (LogIndex i = next_idx; i <= log_.LastIndex(); i++) {
                auto entry = log_.At(i);
                if (entry) entries_to_send.push_back(*entry);
            }
            leader_commit = commit_index_;
        }
        replication_in_flight_.insert(peer_id);
    }

    if (needs_snapshot) {
        SendInstallSnapshot(peer_id, addr, term);
        return;
    }

    size_t sent_count = entries_to_send.size();
    std::string encoded =
        EncodeAppendEntriesRequest({term, id_, prev_log_index, prev_log_term, std::move(entries_to_send), leader_commit});

    std::thread([this, peer_id, addr, encoded, term, prev_log_index, sent_count] {
        std::string response;
        try {
            response = RaftTransport::SendRequest(addr.host, addr.port, encoded, kRpcTimeoutMs);
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            replication_in_flight_.erase(peer_id);
            return;  // peer unreachable right now - this round is simply skipped for it
        }
        AppendEntriesResponse resp = DecodeAppendEntriesResponse(response);

        std::lock_guard<std::mutex> lock(mutex_);
        replication_in_flight_.erase(peer_id);
        if (!peers_.count(peer_id)) return;  // this peer isn't part of the cluster anymore
        if (resp.term > state_.current_term()) {
            BecomeFollower(resp.term);
            return;
        }
        if (role_ != Role::kLeader || term != state_.current_term()) {
            return;  // stale response from a term/role we've already left
        }

        if (resp.success) {
            LogIndex new_match = prev_log_index + sent_count;
            if (new_match > match_index_[peer_id]) match_index_[peer_id] = new_match;
            next_index_[peer_id] = match_index_[peer_id] + 1;

            MaybeAdvanceCommitIndex();
        } else {
            // The follower's consistency check failed - back off by one
            // and retry on the next replication round. A real
            // implementation uses the conflicting term the follower
            // reports to jump back further in a single step; this
            // linear backoff is simpler and still correct, just slower
            // to converge against a long-diverged follower.
            LogIndex& next_idx = next_index_[peer_id];
            if (next_idx > 1) next_idx--;
        }
    }).detach();
}

void RaftNode::SendInstallSnapshot(NodeId peer_id, const PeerAddress& addr, Term term) {
    LogIndex snapshot_index;
    Term snapshot_term;
    std::string data;
    std::map<NodeId, PeerAddress> membership;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader || term != state_.current_term()) {
            replication_in_flight_.erase(peer_id);  // ReplicateTo marked it in-flight before calling us
            return;
        }

        // The declared boundary must be exactly what the dump reflects,
        // not log_.snapshot_index() (which only tracks how far our *log*
        // has been trimmed, and can lag behind how far the state machine
        // has actually been applied - ApplyCommitted() updates last_applied_
        // on every commit, while CompactTo() only runs every
        // kCompactionThreshold entries). Understating it would make the
        // follower re-receive - and re-apply - entries whose effect is
        // already sitting in this dump; SQL statements like INSERT
        // aren't idempotent (a duplicate primary key fails), so that's
        // not just wasted work, it's a real error. Calling
        // snapshot_callback_ here (under mutex_) is what makes this
        // atomic: ApplyCommitted() also needs mutex_, so no commit can
        // land between reading last_applied_ and taking the dump.
        snapshot_index = last_applied_;
        snapshot_term = log_.TermAt(snapshot_index);
        data = snapshot_callback_();

        // Self-inclusive from the sender's point of view: a follower
        // installing a snapshot may have no relevant local log to derive
        // membership from at all (see HandleInstallSnapshot), so it needs
        // to be told every current member's address, including ours -
        // nothing else tells it how to reach the leader itself.
        membership = peers_;
        membership[id_] = own_address_;
    }

    std::string encoded = EncodeInstallSnapshotRequest({term, id_, snapshot_index, snapshot_term, data, membership});

    std::thread([this, peer_id, addr, encoded, term, snapshot_index] {
        std::string response;
        try {
            response = RaftTransport::SendRequest(addr.host, addr.port, encoded, kSnapshotRpcTimeoutMs);
        } catch (...) {
            std::lock_guard<std::mutex> lock(mutex_);
            replication_in_flight_.erase(peer_id);
            return;  // peer unreachable right now - retried on a later round
        }
        InstallSnapshotResponse resp = DecodeInstallSnapshotResponse(response);

        std::lock_guard<std::mutex> lock(mutex_);
        replication_in_flight_.erase(peer_id);
        if (!peers_.count(peer_id)) return;  // this peer isn't part of the cluster anymore
        if (resp.term > state_.current_term()) {
            BecomeFollower(resp.term);
            return;
        }
        if (role_ != Role::kLeader || term != state_.current_term()) return;

        // InstallSnapshot has no consistency check to fail the way
        // AppendEntries does - the follower just adopts it wholesale -
        // so any response means it now has everything through
        // snapshot_index. Resume normal AppendEntries from there.
        next_index_[peer_id] = snapshot_index + 1;
        match_index_[peer_id] = snapshot_index;
    }).detach();
}

InstallSnapshotResponse RaftNode::HandleInstallSnapshot(const InstallSnapshotRequest& req) {
    // Held for the whole call (including the restore below) so two
    // overlapping InstallSnapshot RPCs on this node can never race each
    // other - see the member's doc comment in node.h.
    std::lock_guard<std::mutex> install_lock(install_mutex_);

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (req.term < state_.current_term()) {
            return {state_.current_term()};
        }
        if (req.term > state_.current_term() || role_ != Role::kFollower) {
            BecomeFollower(req.term);
        }
        ResetElectionDeadline();
        last_leader_contact_ = std::chrono::steady_clock::now();
        leader_hint_ = req.leader_id;

        if (req.last_included_index <= log_.snapshot_index()) {
            return {state_.current_term()};  // we're already at least this far along - nothing to do
        }
    }

    // Restoring replaces the whole state machine wholesale, so it's done
    // outside mutex_: it can be slow (real I/O over however much data is
    // in the snapshot) and must not block this node's ability to handle
    // other RPCs (elections, AppendEntries from this same leader) in the
    // meantime.
    restore_callback_(req.data);

    std::lock_guard<std::mutex> lock(mutex_);
    if (req.last_included_index > log_.snapshot_index()) {
        log_.CompactTo(req.last_included_index, req.last_included_term);
        if (req.last_included_index > commit_index_) commit_index_ = req.last_included_index;
        if (req.last_included_index > last_applied_) {
            last_applied_ = req.last_included_index;
            // restore_callback_ already replaced the whole state machine
            // wholesale with this snapshot, so last_applied_ must be
            // durable now too - otherwise a restart right after this call
            // would fall back to the old (lower) log_.applied_index() and
            // re-run apply_callback_ for entries this snapshot's data
            // already reflects.
            log_.SetAppliedIndex(last_applied_);
        }

        // A follower installing a snapshot may have no usable local log
        // to fold ConfChangeCommands from at all (CompactTo may have just
        // discarded everything - see its "badly diverged" branch), so
        // membership is adopted wholesale from the leader's dump instead,
        // exactly like `data` itself.
        bool self_member = req.membership.count(id_) > 0;
        std::map<NodeId, PeerAddress> stripped = req.membership;
        auto self_it = stripped.find(id_);
        // The leader's dump is self-inclusive (see SendInstallSnapshot), so
        // if it still considers this node a member, its entry is this
        // node's own address as the rest of the cluster knows it - more
        // trustworthy than whatever this run's own_address_ currently
        // holds (which may just be the raft_main.cpp placeholder for a
        // beyond-first-run node - see the constructor's own comment).
        if (self_it != stripped.end()) own_address_ = self_it->second;
        stripped.erase(id_);
        membership_state_.Set(std::move(stripped), self_member, own_address_);
        RefreshMembership();

        commit_cv_.notify_all();
    }
    return {state_.current_term()};
}

ReadResponse RaftNode::HandleReadRequest(const ReadRequest& req) {
    // No leader check, no lock on Raft state at all: a read doesn't
    // touch the log or role, just the state machine, and read_callback_
    // is responsible for its own synchronization against concurrent
    // applies (see the engine_mutex wiring in raft_main.cpp).
    try {
        return {false, read_callback_(req.query)};
    } catch (const std::exception& e) {
        return {true, e.what()};
    }
}

DescribeClusterResponse RaftNode::HandleDescribeCluster(const DescribeClusterRequest&) {
    // No leader check, like HandleReadRequest - any node can answer from
    // its own current peers_, no consensus needed. Lets a client discover
    // the rest of the cluster starting from just this one node's address.
    std::lock_guard<std::mutex> lock(mutex_);
    return {id_, own_address_, peers_};
}

bool RaftNode::Propose(const std::string& command, LogIndex* out_index, int timeout_ms, std::string* out_apply_error) {
    LogIndex index;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader) return false;

        // Validated inside this same critical section, not as a separate
        // pre-check before it: doing it separately would reopen a race
        // between two concurrent conf-change proposals landing on this
        // leader at once.
        if (auto cc = TryDecodeConfChange(command)) {
            bool pending = false;
            for (LogIndex i = commit_index_ + 1; i <= log_.LastIndex() && !pending; i++) {
                auto entry = log_.At(i);
                if (entry && TryDecodeConfChange(entry->command)) pending = true;
            }
            if (pending) {
                if (out_apply_error) *out_apply_error = "a membership change is already pending";
                return false;
            }
            if (cc->type == ConfChangeType::kAddServer) {
                if (cc->server_id == id_ || peers_.count(cc->server_id)) {
                    if (out_apply_error) {
                        *out_apply_error = "server " + std::to_string(cc->server_id) + " is already a member";
                    }
                    return false;
                }
            } else {  // kRemoveServer
                if (cc->server_id != id_ && !peers_.count(cc->server_id)) {
                    if (out_apply_error) {
                        *out_apply_error = "server " + std::to_string(cc->server_id) + " is not a member";
                    }
                    return false;
                }
            }
        }

        index = log_.Append(state_.current_term(), command);
        RefreshMembership();
        MaybeAdvanceCommitIndex();  // handles the single-node-cluster case immediately
    }
    if (out_index) *out_index = index;

    ReplicateToAll();  // don't wait for the next heartbeat tick - replicate this write right away

    std::unique_lock<std::mutex> lock(mutex_);
    commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                         [this, index] { return commit_index_ >= index || role_ != Role::kLeader; });
    bool committed = commit_index_ >= index;
    if (committed && out_apply_error) {
        auto it = apply_errors_.find(index);
        if (it != apply_errors_.end()) *out_apply_error = it->second;
    }
    return committed;
}

ClientResponse RaftNode::HandleClientRequest(const ClientRequest& req) {
    if (role() != Role::kLeader) {
        return {false, 0, true, leader_hint(), "not the leader"};
    }
    LogIndex index = 0;
    std::string apply_error;
    bool ok = Propose(req.command, &index, 2000, &apply_error);
    if (!ok) {
        if (!apply_error.empty()) {
            // Propose() rejected this before ever appending it (e.g. a
            // pending membership change, or an invalid add/remove
            // target) - a definite, immediate failure, not a timeout.
            return {false, 0, false, id_, apply_error};
        }
        // Either the commit wasn't confirmed within the timeout, or
        // this node stopped being leader partway through - either way,
        // from the caller's perspective the outcome is unconfirmed, not
        // a definite failure.
        return {false, 0, role() != Role::kLeader, leader_hint(), "propose failed or timed out"};
    }
    if (!apply_error.empty()) {
        // Committed - every replica durably logged this command and
        // will run it identically - but running it failed (e.g.
        // duplicate primary key). That's a definite, not a timeout-y,
        // failure: unlike the branch above, retrying this exact command
        // would just fail the same way again everywhere.
        return {false, index, false, id_, apply_error};
    }
    return {true, index, false, id_, ""};
}

ClientResponse RaftNode::ProposeOrForward(const std::string& command, int timeout_ms) {
    if (role() == Role::kLeader) {
        return HandleClientRequest({command});
    }

    NodeId target = leader_hint();
    for (int hop = 0; hop < kMaxForwardHops; hop++) {
        if (target == 0) {
            return {false, 0, true, 0, "no known leader to forward to"};
        }
        if (target == id_) {
            // Our own hint points back at ourselves despite role() just
            // having said we're not the leader - a race with a
            // concurrent election. Re-check rather than forward to
            // ourselves in a loop.
            if (role() == Role::kLeader) return HandleClientRequest({command});
            return {false, 0, true, 0, "leader hint pointed back to self while not leader"};
        }

        auto it = peers_.find(target);
        if (it == peers_.end()) {
            return {false, 0, true, target, "unknown leader node id " + std::to_string(target)};
        }

        std::string encoded = EncodeClientRequest({command});
        std::string response;
        try {
            response = RaftTransport::SendRequest(it->second.host, it->second.port, encoded, timeout_ms);
        } catch (...) {
            return {false, 0, true, target, "could not reach node " + std::to_string(target)};
        }

        ClientResponse resp = DecodeClientResponse(response);
        if (resp.success || !resp.not_leader) {
            return resp;  // done - either it committed, or it failed for a reason other than "wrong node"
        }
        if (resp.leader_hint == 0 || resp.leader_hint == target) {
            return resp;  // that node has no better lead than the one we just tried - give up
        }
        target = resp.leader_hint;  // it pointed us further - follow it, up to kMaxForwardHops
    }
    return {false, 0, true, target, "too many redirects while locating the leader"};
}

}  // namespace distdb
