#pragma once

#include <optional>
#include <string>
#include <vector>

#include "message.h"
#include "types.h"

namespace distdb {

// The replicated log: an ordered, 1-indexed sequence of (term, command)
// entries, plus a boundary (snapshot_index_/snapshot_term_) marking how
// much of the earlier history has been compacted away. Persisted by
// rewriting the whole log file on every mutation (write-to-`.tmp`-then-
// rename, the same crash-safety trick SSTableWriter::Finish() uses) -
// simple and always crash-safe, at the cost of O(log size) work per
// mutation instead of O(1). Compaction is what keeps that "log size"
// bounded instead of growing with the cluster's entire history.
class RaftLog {
 public:
    explicit RaftLog(std::string path);

    // Loads any prior entries (and the snapshot boundary) from disk;
    // leaves the log empty if the file doesn't exist (a node that has
    // never run before).
    void Load();

    // 0/0 if this log has never been compacted.
    LogIndex snapshot_index() const { return snapshot_index_; }
    Term snapshot_term() const { return snapshot_term_; }

    // The highest index actually handed to the application via
    // apply_callback_ - persisted separately from snapshot_index_ because
    // compaction only runs every kCompactionThreshold entries, so on
    // restart there can be a handful of already-applied entries that
    // haven't been compacted away yet. Without this, RaftNode would only
    // know "applied at least up to snapshot_index_" and would re-run
    // apply_callback_ on that already-applied gap once commit_index_
    // advances again - harmless for an INSERT (rejected as a duplicate
    // key) but a real double-apply for something like an UPDATE counter.
    // Falls back to snapshot_index_ when reading a log file written
    // before this field existed (see Load()).
    LogIndex applied_index() const { return applied_index_; }
    void SetAppliedIndex(LogIndex index);

    LogIndex LastIndex() const;  // snapshot_index() if there are no entries past it
    Term LastTerm() const;       // snapshot_term() if there are no entries past it
    Term TermAt(LogIndex index) const;              // 0 if index is before the snapshot or past the end
    std::optional<LogEntry> At(LogIndex index) const;  // nullopt likewise

    // Leader-side: appends one new entry at the end, persists, and
    // returns its index.
    LogIndex Append(Term term, std::string command);

    // Follower-side: validates that entry `prev_log_index` in this log
    // has term `prev_log_term` (or prev_log_index is 0, meaning "start
    // from the very beginning"), then makes entries [prev_log_index+1 ..]
    // match `entries` exactly, truncating any conflicting suffix first.
    // An entry that already matches (same index, same term - Raft's log
    // matching property guarantees that means the same command too) is
    // left alone, so retransmitted/duplicate AppendEntries are handled
    // safely. Returns false if the prev_log_index/prev_log_term check
    // fails, meaning the leader must retry with an earlier prev_log_index.
    bool AppendEntriesFrom(LogIndex prev_log_index, Term prev_log_term, const std::vector<LogEntry>& entries);

    // Discards every entry up to and including `last_included_index`,
    // recording that this log's history before that point is now
    // represented by a snapshot instead (of term `last_included_term`).
    // If this log still holds that exact (index, term) pair, whatever
    // comes after it is kept - the common case, whether compacting our
    // own already-applied history or catching up a follower that's only
    // slightly behind. Otherwise every existing entry is discarded, since
    // none of them can be trusted to agree with the snapshot - the case
    // for a follower that's badly diverged or far behind, installing a
    // leader's InstallSnapshot. A no-op if `last_included_index` is at or
    // before the current snapshot boundary (already compacted at least
    // that far).
    void CompactTo(LogIndex last_included_index, Term last_included_term);

 private:
    void Save();

    std::string path_;
    LogIndex snapshot_index_ = 0;
    Term snapshot_term_ = 0;
    std::vector<LogEntry> entries_;  // entries_[i] holds log index snapshot_index_ + 1 + i
    LogIndex applied_index_ = 0;
};

}  // namespace distdb
