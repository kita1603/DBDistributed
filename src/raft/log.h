#pragma once

#include <optional>
#include <string>
#include <vector>

#include "message.h"
#include "types.h"

namespace distdb {

// The replicated log: an ordered, 1-indexed sequence of (term, command)
// entries. Persisted by rewriting the whole log file on every mutation
// (write-to-`.tmp`-then-rename, the same crash-safety trick
// SSTableWriter::Finish() uses) - simple and always crash-safe, at the
// cost of O(log size) work per append instead of O(1). Fine at this
// project's scale; a real implementation appends incrementally and only
// rewrites the file via periodic snapshotting.
class RaftLog {
 public:
    explicit RaftLog(std::string path);

    // Loads any prior entries from disk; leaves the log empty if the
    // file doesn't exist (a node that has never run before).
    void Load();

    LogIndex LastIndex() const;
    Term LastTerm() const;
    Term TermAt(LogIndex index) const;  // 0 if index is 0 or past the end
    std::optional<LogEntry> At(LogIndex index) const;

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

 private:
    void Save();

    std::string path_;
    std::vector<LogEntry> entries_;  // entries_[i] holds log index i+1
};

}  // namespace distdb
