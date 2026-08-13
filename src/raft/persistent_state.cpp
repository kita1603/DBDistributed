#include "persistent_state.h"

#include <filesystem>
#include <fstream>

namespace distdb {

PersistentState::PersistentState(std::string path) : path_(std::move(path)) {}

void PersistentState::Load() {
    std::ifstream file(path_);
    if (!file.is_open()) return;  // never run before - defaults are correct

    uint64_t term = 0;
    file >> term;
    current_term_ = term;

    int64_t voted_for_raw = -1;
    file >> voted_for_raw;
    voted_for_ = (voted_for_raw < 0) ? std::nullopt : std::optional<NodeId>(static_cast<NodeId>(voted_for_raw));
}

void PersistentState::Set(Term term, std::optional<NodeId> voted_for) {
    current_term_ = term;
    voted_for_ = voted_for;

    // Write to a temp file and rename over the real one, so a crash
    // mid-write leaves the old (still-valid) state file intact instead
    // of a half-written one - the same crash-safety trick
    // SSTableWriter::Finish() uses for publishing a finished table.
    std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream file(tmp_path, std::ios::trunc);
        file << current_term_ << ' ' << (voted_for_ ? static_cast<int64_t>(*voted_for_) : -1);
    }
    std::filesystem::rename(tmp_path, path_);
}

}  // namespace distdb
