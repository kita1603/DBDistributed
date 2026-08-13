#include "engine.h"

#include <algorithm>
#include <filesystem>
#include <iomanip>
#include <map>
#include <sstream>

namespace distdb {

namespace {

// Kept deliberately small so a flush or compaction can be triggered by
// hand with just a few REPL commands while testing. A real engine sizes
// these off the memtable's byte footprint (tens of MB) and an SSTable
// count/size compaction policy (leveled or tiered), not raw entry counts.
constexpr size_t kMemtableFlushThreshold = 4;
constexpr size_t kCompactionTriggerCount = 4;

}  // namespace

StorageEngine::StorageEngine(std::string db_path)
    : db_path_(std::move(db_path)), wal_(db_path_ + "/wal.log") {}

std::string StorageEngine::SSTablePath(uint64_t seq) const {
    std::ostringstream oss;
    oss << db_path_ << "/" << std::setfill('0') << std::setw(6) << seq << ".sst";
    return oss.str();
}

void StorageEngine::Open() {
    std::filesystem::create_directories(db_path_);

    // Discover existing SSTables, newest sequence number first. Any
    // stray *.sst.tmp is the leftover of a flush that crashed before its
    // atomic rename - it never became a visible table, so it's safe to
    // delete.
    std::vector<std::pair<uint64_t, std::string>> found;
    for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
        const auto& path = entry.path();
        if (path.extension() == ".tmp") {
            std::filesystem::remove(path);
            continue;
        }
        if (path.extension() != ".sst") continue;
        found.emplace_back(std::stoull(path.stem().string()), path.string());
    }
    std::sort(found.begin(), found.end(), [](const auto& a, const auto& b) { return a.first > b.first; });

    for (const auto& [seq, path] : found) {
        auto reader = std::make_unique<SSTableReader>(path);
        reader->Open();
        sstables_.push_back(std::move(reader));
        next_sstable_seq_ = std::max(next_sstable_seq_, seq + 1);
    }

    wal_.Open();
    wal_.Replay([this](RecordType type, const std::string& key, const std::string& value) {
        if (type == RecordType::kPut) {
            memtable_.Put(key, value);
        } else {
            memtable_.Delete(key);
        }
    });
}

void StorageEngine::Put(const std::string& key, const std::string& value) {
    wal_.AppendPut(key, value);
    memtable_.Put(key, value);
    if (memtable_.size() >= kMemtableFlushThreshold) FlushMemtable();
}

void StorageEngine::Delete(const std::string& key) {
    wal_.AppendDelete(key);
    memtable_.Delete(key);
    if (memtable_.size() >= kMemtableFlushThreshold) FlushMemtable();
}

std::optional<std::string> StorageEngine::Get(const std::string& key) const {
    auto result = memtable_.Lookup(key);
    if (result.status == LookupStatus::kFound) return result.value;
    if (result.status == LookupStatus::kDeleted) return std::nullopt;

    for (const auto& table : sstables_) {
        auto table_result = table->Lookup(key);
        if (table_result.status == LookupStatus::kFound) return table_result.value;
        if (table_result.status == LookupStatus::kDeleted) return std::nullopt;
    }
    return std::nullopt;
}

std::vector<std::pair<std::string, std::string>> StorageEngine::Scan(const std::string& prefix) const {
    // Same "first-seen-wins" merge as MaybeCompact, but scoped to a
    // prefix and starting from the live memtable rather than only
    // SSTables.
    std::map<std::string, std::optional<std::string>> merged;
    for (auto& [key, value] : memtable_.Scan(prefix)) merged.emplace(key, value);
    for (const auto& table : sstables_) {
        for (auto& [key, value] : table->Scan(prefix)) merged.emplace(key, value);
    }

    std::vector<std::pair<std::string, std::string>> result;
    for (auto& [key, value] : merged) {
        if (value.has_value()) result.emplace_back(key, *value);
    }
    return result;
}

void StorageEngine::FlushMemtable() {
    if (memtable_.size() == 0) return;

    std::string path = SSTablePath(next_sstable_seq_++);
    SSTableWriter writer(path);
    for (const auto& [key, value] : memtable_.Entries()) {
        writer.Add(key, value);
    }
    writer.Finish();

    auto reader = std::make_unique<SSTableReader>(path);
    reader->Open();
    sstables_.insert(sstables_.begin(), std::move(reader));

    memtable_.Clear();
    // The memtable's data is now durable in the SSTable, so the WAL
    // records that rebuilt it are no longer needed for recovery.
    wal_.Reset();

    MaybeCompact();
}

void StorageEngine::MaybeCompact() {
    if (sstables_.size() < kCompactionTriggerCount) return;

    // Walk newest -> oldest and keep only the first value seen per key.
    // std::map::emplace is a no-op if the key is already present, which
    // is exactly "don't let an older table overwrite a newer value".
    std::map<std::string, std::optional<std::string>> merged;
    std::vector<std::string> old_paths;
    for (const auto& table : sstables_) {
        old_paths.push_back(table->path());
        for (auto& [key, value] : table->ReadAll()) {
            merged.emplace(std::move(key), std::move(value));
        }
    }

    std::string path = SSTablePath(next_sstable_seq_++);
    SSTableWriter writer(path);
    for (const auto& [key, value] : merged) {
        // This merge spans every SSTable that currently exists, so if a
        // key's newest state is a tombstone, no older table survives that
        // could still need to see it - safe to drop for good.
        if (!value.has_value()) continue;
        writer.Add(key, value);
    }
    writer.Finish();

    auto reader = std::make_unique<SSTableReader>(path);
    reader->Open();

    sstables_.clear();
    sstables_.push_back(std::move(reader));

    for (const auto& old_path : old_paths) {
        std::filesystem::remove(old_path);
    }
}

}  // namespace distdb
