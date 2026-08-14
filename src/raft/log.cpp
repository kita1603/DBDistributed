#include "log.h"

#include <filesystem>
#include <fstream>

namespace distdb {

namespace {

template <typename T>
void WriteRaw(std::ofstream& out, const T& v) {
    out.write(reinterpret_cast<const char*>(&v), sizeof(v));
}

template <typename T>
void ReadRaw(std::ifstream& in, T& v) {
    in.read(reinterpret_cast<char*>(&v), sizeof(v));
}

}  // namespace

RaftLog::RaftLog(std::string path) : path_(std::move(path)) {}

void RaftLog::Load() {
    std::ifstream file(path_, std::ios::binary);
    if (!file.is_open()) return;  // never run before - starts empty

    ReadRaw(file, snapshot_index_);
    ReadRaw(file, snapshot_term_);

    uint32_t count = 0;
    ReadRaw(file, count);
    entries_.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        LogEntry e;
        ReadRaw(file, e.index);
        ReadRaw(file, e.term);
        uint32_t len = 0;
        ReadRaw(file, len);
        e.command.resize(len);
        if (len > 0) file.read(e.command.data(), len);
        entries_.push_back(std::move(e));
    }

    // Trailing field, added after this format shipped - a log file
    // written before applied_index_ existed simply ends here. Falling
    // back to snapshot_index_ (everything compacted is definitely
    // applied) is the best available answer for such a file, same as
    // before this field was introduced.
    ReadRaw(file, applied_index_);
    if (!file) applied_index_ = snapshot_index_;
}

LogIndex RaftLog::LastIndex() const { return entries_.empty() ? snapshot_index_ : entries_.back().index; }
Term RaftLog::LastTerm() const { return entries_.empty() ? snapshot_term_ : entries_.back().term; }

Term RaftLog::TermAt(LogIndex index) const {
    if (index == snapshot_index_) return snapshot_term_;
    if (index < snapshot_index_ || index > LastIndex()) return 0;
    return entries_[index - snapshot_index_ - 1].term;
}

std::optional<LogEntry> RaftLog::At(LogIndex index) const {
    if (index <= snapshot_index_ || index > LastIndex()) return std::nullopt;
    return entries_[index - snapshot_index_ - 1];
}

LogIndex RaftLog::Append(Term term, std::string command) {
    LogEntry e;
    e.index = LastIndex() + 1;
    e.term = term;
    e.command = std::move(command);
    entries_.push_back(std::move(e));
    Save();
    return entries_.back().index;
}

bool RaftLog::AppendEntriesFrom(LogIndex prev_log_index, Term prev_log_term, const std::vector<LogEntry>& entries) {
    if (prev_log_index > 0 && TermAt(prev_log_index) != prev_log_term) {
        return false;
    }

    bool changed = false;
    for (size_t i = 0; i < entries.size(); i++) {
        LogIndex idx = prev_log_index + 1 + i;
        if (idx <= snapshot_index_) continue;  // already compacted away - definitely already applied

        size_t pos = idx - snapshot_index_ - 1;
        if (pos < entries_.size()) {
            if (entries_[pos].term == entries[i].term) {
                continue;  // already have this exact entry - a retransmission, nothing to do
            }
            entries_.resize(pos);  // conflict - drop this entry and everything after it
        }
        LogEntry e = entries[i];
        e.index = idx;
        entries_.push_back(std::move(e));
        changed = true;
    }

    if (changed) Save();
    return true;
}

void RaftLog::CompactTo(LogIndex last_included_index, Term last_included_term) {
    if (last_included_index <= snapshot_index_) return;  // already compacted at least this far

    if (last_included_index <= LastIndex() && TermAt(last_included_index) == last_included_term) {
        size_t drop_count = last_included_index - snapshot_index_;
        entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(drop_count));
    } else {
        entries_.clear();
    }
    snapshot_index_ = last_included_index;
    snapshot_term_ = last_included_term;
    Save();
}

void RaftLog::Save() {
    std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream file(tmp_path, std::ios::binary | std::ios::trunc);
        WriteRaw(file, snapshot_index_);
        WriteRaw(file, snapshot_term_);
        uint32_t count = static_cast<uint32_t>(entries_.size());
        WriteRaw(file, count);
        for (const auto& e : entries_) {
            WriteRaw(file, e.index);
            WriteRaw(file, e.term);
            uint32_t len = static_cast<uint32_t>(e.command.size());
            WriteRaw(file, len);
            file.write(e.command.data(), static_cast<std::streamsize>(e.command.size()));
        }
        WriteRaw(file, applied_index_);
    }
    std::filesystem::rename(tmp_path, path_);
}

void RaftLog::SetAppliedIndex(LogIndex index) {
    applied_index_ = index;
    Save();
}

}  // namespace distdb
