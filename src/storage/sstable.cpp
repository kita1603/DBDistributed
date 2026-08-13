#include "sstable.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <stdexcept>

namespace distdb {

namespace {

// Sentinel value_len marking a tombstone, so no value bytes follow it in
// the data block. A real value can never reach this length in practice.
constexpr uint32_t kTombstone = std::numeric_limits<uint32_t>::max();

// Written at the very end of a finished file; Open() checks it so a
// truncated or otherwise corrupt file is rejected instead of silently
// misread.
constexpr uint32_t kMagic = 0x53535444u;  // "SSTD"

template <typename T>
void WriteRaw(std::ofstream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

template <typename T>
void ReadRaw(std::ifstream& in, T& value) {
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
}

bool StartsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

}  // namespace

SSTableWriter::SSTableWriter(std::string path) : path_(std::move(path)), tmp_path_(path_ + ".tmp") {
    file_.open(tmp_path_, std::ios::out | std::ios::binary | std::ios::trunc);
    if (!file_.is_open()) {
        throw std::runtime_error("failed to create SSTable temp file: " + tmp_path_);
    }
}

void SSTableWriter::Add(const std::string& key, const std::optional<std::string>& value) {
    uint64_t offset = static_cast<uint64_t>(file_.tellp());

    WriteRaw(file_, static_cast<uint32_t>(key.size()));
    file_.write(key.data(), static_cast<std::streamsize>(key.size()));

    if (value.has_value()) {
        WriteRaw(file_, static_cast<uint32_t>(value->size()));
        file_.write(value->data(), static_cast<std::streamsize>(value->size()));
    } else {
        WriteRaw(file_, kTombstone);
    }

    index_.emplace_back(key, offset);
}

void SSTableWriter::Finish() {
    uint64_t index_offset = static_cast<uint64_t>(file_.tellp());
    for (const auto& [key, offset] : index_) {
        WriteRaw(file_, static_cast<uint32_t>(key.size()));
        file_.write(key.data(), static_cast<std::streamsize>(key.size()));
        WriteRaw(file_, offset);
    }

    WriteRaw(file_, index_offset);
    WriteRaw(file_, static_cast<uint32_t>(index_.size()));
    WriteRaw(file_, kMagic);

    file_.flush();
    file_.close();

    std::filesystem::rename(tmp_path_, path_);
}

SSTableReader::SSTableReader(std::string path) : path_(std::move(path)) {}

void SSTableReader::Open() {
    file_.open(path_, std::ios::in | std::ios::binary);
    if (!file_.is_open()) {
        throw std::runtime_error("failed to open SSTable: " + path_);
    }

    constexpr uint64_t kFooterSize = sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t);
    uint64_t file_size = std::filesystem::file_size(path_);
    if (file_size < kFooterSize) {
        throw std::runtime_error("SSTable too small to contain a footer: " + path_);
    }

    file_.seekg(static_cast<std::streamoff>(file_size - kFooterSize));
    uint64_t index_offset = 0;
    uint32_t entry_count = 0;
    uint32_t magic = 0;
    ReadRaw(file_, index_offset);
    ReadRaw(file_, entry_count);
    ReadRaw(file_, magic);
    if (magic != kMagic) {
        throw std::runtime_error("SSTable footer magic mismatch (truncated/corrupt file): " + path_);
    }

    data_end_ = index_offset;

    file_.seekg(static_cast<std::streamoff>(index_offset));
    index_.reserve(entry_count);
    for (uint32_t i = 0; i < entry_count; i++) {
        uint32_t key_len = 0;
        ReadRaw(file_, key_len);
        std::string key(key_len, '\0');
        file_.read(key.data(), key_len);
        uint64_t offset = 0;
        ReadRaw(file_, offset);
        index_.emplace_back(std::move(key), offset);
    }
}

LookupResult SSTableReader::Lookup(const std::string& key) const {
    auto it = std::lower_bound(index_.begin(), index_.end(), key,
                                [](const auto& entry, const std::string& k) { return entry.first < k; });
    if (it == index_.end() || it->first != key) {
        return {LookupStatus::kNotFound, ""};
    }

    file_.seekg(static_cast<std::streamoff>(it->second));
    uint32_t key_len = 0;
    ReadRaw(file_, key_len);
    file_.seekg(key_len, std::ios::cur);  // skip the key bytes, already matched via the index

    uint32_t value_len = 0;
    ReadRaw(file_, value_len);
    if (value_len == kTombstone) {
        return {LookupStatus::kDeleted, ""};
    }

    std::string value(value_len, '\0');
    file_.read(value.data(), value_len);
    return {LookupStatus::kFound, std::move(value)};
}

std::vector<std::pair<std::string, std::optional<std::string>>> SSTableReader::Scan(const std::string& prefix) const {
    std::vector<std::pair<std::string, std::optional<std::string>>> result;

    auto it = std::lower_bound(index_.begin(), index_.end(), prefix,
                                [](const auto& entry, const std::string& p) { return entry.first < p; });
    for (; it != index_.end() && StartsWith(it->first, prefix); ++it) {
        file_.seekg(static_cast<std::streamoff>(it->second));
        uint32_t key_len = 0;
        ReadRaw(file_, key_len);
        file_.seekg(key_len, std::ios::cur);  // skip the key bytes, already known from the index

        uint32_t value_len = 0;
        ReadRaw(file_, value_len);
        if (value_len == kTombstone) {
            result.emplace_back(it->first, std::nullopt);
        } else {
            std::string value(value_len, '\0');
            file_.read(value.data(), value_len);
            result.emplace_back(it->first, std::move(value));
        }
    }

    return result;
}

std::vector<std::pair<std::string, std::optional<std::string>>> SSTableReader::ReadAll() const {
    std::vector<std::pair<std::string, std::optional<std::string>>> result;
    file_.seekg(0);

    while (static_cast<uint64_t>(file_.tellg()) < data_end_) {
        uint32_t key_len = 0;
        ReadRaw(file_, key_len);
        std::string key(key_len, '\0');
        file_.read(key.data(), key_len);

        uint32_t value_len = 0;
        ReadRaw(file_, value_len);
        if (value_len == kTombstone) {
            result.emplace_back(std::move(key), std::nullopt);
        } else {
            std::string value(value_len, '\0');
            file_.read(value.data(), value_len);
            result.emplace_back(std::move(key), std::move(value));
        }
    }

    return result;
}

}  // namespace distdb
