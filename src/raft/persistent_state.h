#pragma once

#include <optional>
#include <string>

#include "types.h"

namespace distdb {

// Raft's "hard state": currentTerm and votedFor. Both must be durable
// before a RequestVote/AppendEntries reply is sent - losing them across
// a crash could let a restarted node vote twice in the same term (a
// safety violation) or forget it already has a leader for its current
// term. Small enough that Set() just rewrites the whole file rather
// than appending, unlike the (future) log.
class PersistentState {
 public:
    explicit PersistentState(std::string path);

    // Loads prior state from disk if the file exists; otherwise leaves
    // the fresh-node defaults (term 0, no vote), which are correct for
    // a node that has never run before.
    void Load();

    Term current_term() const { return current_term_; }
    std::optional<NodeId> voted_for() const { return voted_for_; }

    // Updates both fields and persists them to disk before returning.
    void Set(Term term, std::optional<NodeId> voted_for);

 private:
    std::string path_;
    Term current_term_ = 0;
    std::optional<NodeId> voted_for_;
};

}  // namespace distdb
