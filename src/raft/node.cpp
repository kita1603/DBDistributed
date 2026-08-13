#include "node.h"

#include <algorithm>
#include <iostream>
#include <random>
#include <thread>

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

RaftNode::RaftNode(NodeId id, uint16_t listen_port, std::map<NodeId, PeerAddress> peers, std::string state_dir,
                    ApplyCallback apply_callback, SnapshotCallback snapshot_callback,
                    RestoreCallback restore_callback)
    : id_(id),
      listen_port_(listen_port),
      peers_(std::move(peers)),
      apply_callback_(std::move(apply_callback)),
      snapshot_callback_(std::move(snapshot_callback)),
      restore_callback_(std::move(restore_callback)),
      state_(state_dir + "/raft_state_" + std::to_string(id) + ".txt"),
      log_(state_dir + "/raft_log_" + std::to_string(id) + ".bin"),
      transport_(listen_port) {
    state_.Load();
    log_.Load();
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
    role_ = Role::kLeader;
    leader_hint_ = id_;
    next_heartbeat_time_ = std::chrono::steady_clock::now();  // fire the first replication round on the next tick
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
        if (entry) apply_callback_(entry->index, entry->command);
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
        log_.CompactTo(last_applied_, log_.TermAt(last_applied_));
        std::cout << "[node " << id_ << "] compacted log: snapshot index " << old_snapshot_index << " -> "
                  << log_.snapshot_index() << "\n";
    }
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
    std::lock_guard<std::mutex> lock(mutex_);

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
    leader_hint_ = req.leader_id;

    bool ok = log_.AppendEntriesFrom(req.prev_log_index, req.prev_log_term, req.entries);
    if (!ok) {
        return {state_.current_term(), false, 0};
    }

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
        state_.Set(state_.current_term() + 1, id_);
        role_ = Role::kCandidate;
        votes_received_ = 1;  // votes for itself
        election_term = state_.current_term();
        last_index = log_.LastIndex();
        last_term = log_.LastTerm();
        ResetElectionDeadline();
        std::cout << "[node " << id_ << "] term " << election_term << ": starting election\n";
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
            return;  // peer unreachable right now - this round is simply skipped for it
        }
        AppendEntriesResponse resp = DecodeAppendEntriesResponse(response);

        std::lock_guard<std::mutex> lock(mutex_);
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

            // Advance commit_index_ to the highest index a majority has
            // acknowledged - but only count it if that entry is from
            // our own current term. Committing an older-term entry
            // directly (rather than it committing as a side effect of a
            // later same-term entry committing) is exactly the unsafe
            // case Raft's Figure 8 warns about.
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
            }
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
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader || term != state_.current_term()) return;

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
    }

    std::string encoded = EncodeInstallSnapshotRequest({term, id_, snapshot_index, snapshot_term, data});

    std::thread([this, peer_id, addr, encoded, term, snapshot_index] {
        std::string response;
        try {
            response = RaftTransport::SendRequest(addr.host, addr.port, encoded, kSnapshotRpcTimeoutMs);
        } catch (...) {
            return;  // peer unreachable right now - retried on a later round
        }
        InstallSnapshotResponse resp = DecodeInstallSnapshotResponse(response);

        std::lock_guard<std::mutex> lock(mutex_);
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
        if (req.last_included_index > last_applied_) last_applied_ = req.last_included_index;
        commit_cv_.notify_all();
    }
    return {state_.current_term()};
}

bool RaftNode::Propose(const std::string& command, LogIndex* out_index, int timeout_ms) {
    LogIndex index;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (role_ != Role::kLeader) return false;
        index = log_.Append(state_.current_term(), command);
    }
    if (out_index) *out_index = index;

    ReplicateToAll();  // don't wait for the next heartbeat tick - replicate this write right away

    std::unique_lock<std::mutex> lock(mutex_);
    commit_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms),
                         [this, index] { return commit_index_ >= index || role_ != Role::kLeader; });
    return commit_index_ >= index;
}

ClientResponse RaftNode::HandleClientRequest(const ClientRequest& req) {
    if (role() != Role::kLeader) {
        return {false, 0, true, leader_hint(), "not the leader"};
    }
    LogIndex index = 0;
    bool ok = Propose(req.command, &index);
    if (!ok) {
        // Either the commit wasn't confirmed within the timeout, or
        // this node stopped being leader partway through - either way,
        // from the caller's perspective the outcome is unconfirmed, not
        // a definite failure.
        return {false, 0, role() != Role::kLeader, leader_hint(), "propose failed or timed out"};
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
