#pragma once

#include <functional>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "txn.h"

namespace distdb {

// This shard's side of two-phase commit: holds the row-key locks and
// staged (not-yet-run) commands for every transaction currently
// prepared here but not yet resolved. Every replica in the shard
// applies the exact same sequence of Prepare/Commit/Abort calls (they
// arrive as ordinary committed Raft log entries), so they all reach the
// same lock/pending state independently - the same determinism argument
// that makes replicating plain SQL statements safe applies here too.
//
// Not thread-safe on its own - the caller (raft_main.cpp's
// apply_callback) already holds engine_mutex for the SQL side of every
// apply, and must hold the same lock across these calls.
//
// Known simplification: all of this lives in memory only. It is not
// part of SnapshotCallback's dump, so a node that restarts (or compacts
// its log) while a transaction is prepared-but-unresolved here forgets
// the lock entirely. For a from-scratch learning implementation this is
// an accepted gap, not a subtle bug - see the Phase 4 section of
// README.md.
class TxnParticipant {
 public:
    // Tries to lock every key in `keys` for `txn_id` and stage `commands`
    // to run later. Returns "" (vote yes) if every key was free, or an
    // error message (vote no) the instant any key is already locked by
    // a *different* still-pending transaction - none of the requested
    // keys get locked in that case. Re-preparing the same txn_id again
    // (e.g. a client retry) is idempotent as long as it names the same
    // keys/commands, since they're already locked by this same txn_id.
    std::string Prepare(TxnId txn_id, std::vector<std::string> keys, std::vector<std::string> commands);

    // Runs every command staged by Prepare() for `txn_id` (via `run`,
    // which does the actual SqlExecutor::Execute call) and releases its
    // locks. `run` isn't expected to throw - apply-time SQL errors here
    // would be a genuine bug, since PREPARE already implies the leader
    // that proposed it believed these commands would succeed - but if
    // one does throw, the locks are still released before it propagates.
    // An unknown txn_id (already resolved earlier, or this replica never
    // saw the PREPARE for some reason) is reported as an error rather
    // than silently ignored, since - unlike Abort - re-running a commit
    // is not something that should ever need to happen.
    std::string Commit(TxnId txn_id, const std::function<void(const std::string&)>& run);

    // Releases txn_id's locks and discards its staged commands without
    // running them. Idempotent: an unknown txn_id (never prepared here,
    // or already resolved) is not an error - a duplicate/retried ABORT
    // must be a safe no-op.
    std::string Abort(TxnId txn_id);

 private:
    struct Pending {
        std::vector<std::string> keys;
        std::vector<std::string> commands;
    };

    void ReleaseLocks(const Pending& pending);

    std::map<TxnId, Pending> pending_;
    std::set<std::string> locked_keys_;
};

}  // namespace distdb
