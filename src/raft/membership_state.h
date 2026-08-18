#pragma once

#include <map>
#include <string>

#include "types.h"

namespace distdb {

// Persisted record of this shard's cluster membership, as of
// RaftLog::snapshot_index() - the compacted-history counterpart to
// RaftLog's own snapshot_index_/snapshot_term_ boundary. Needed for the
// same reason RaftLog::applied_index_ is: a ConfChange entry that gets
// compacted away is gone from RaftLog::At() forever, so whatever it
// changed about membership has to be captured here *before* that happens,
// or it would be forgotten across a restart.
//
// base_peers() excludes this node itself (matching RaftNode::peers_'s own
// convention - a node never RPCs itself), and self_is_member() tracks
// separately whether this node itself is still part of the cluster.
//
// Deliberately its own small class rather than folded into
// PersistentState: that class's job is Raft's hard state (currentTerm,
// votedFor); this is cluster *shape*, a different responsibility.
class MembershipState {
 public:
    explicit MembershipState(std::string path);

    // The conventional path for a node's membership file, given its state
    // directory and id - shared by RaftNode's constructor and by
    // raft_main.cpp's startup logic, which needs to know whether this
    // node has ever run/been-added before *without* constructing a
    // RaftNode first (a node added via add-server isn't listed in
    // routing.conf, but once it's run once its own membership file - not
    // routing.conf - is what's authoritative on every later restart).
    static std::string PathFor(const std::string& state_dir, NodeId id);

    // Loads prior state from disk if the file exists; otherwise leaves
    // base_peers() empty and self_is_member() true - the caller decides
    // how to seed a genuinely first-ever run from there.
    void Load();

    bool HasFile() const { return has_file_; }
    const std::map<NodeId, PeerAddress>& base_peers() const { return base_peers_; }
    bool self_is_member() const { return self_is_member_; }

    // Updates both fields and persists them to disk before returning.
    void Set(std::map<NodeId, PeerAddress> peers, bool self_is_member);

 private:
    std::string path_;
    bool has_file_ = false;
    std::map<NodeId, PeerAddress> base_peers_;
    bool self_is_member_ = true;
};

}  // namespace distdb
