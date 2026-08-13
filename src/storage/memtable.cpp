#include "memtable.h"

namespace distdb {

namespace {

bool StartsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

}  // namespace

void Memtable::Put(const std::string& key, const std::string& value) {
    entries_[key] = value;
}

void Memtable::Delete(const std::string& key) {
    entries_[key] = std::nullopt;
}

LookupResult Memtable::Lookup(const std::string& key) const {
    auto it = entries_.find(key);
    if (it == entries_.end()) return {LookupStatus::kNotFound, ""};
    if (!it->second.has_value()) return {LookupStatus::kDeleted, ""};
    return {LookupStatus::kFound, *it->second};
}

std::vector<std::pair<std::string, std::optional<std::string>>> Memtable::Scan(const std::string& prefix) const {
    std::vector<std::pair<std::string, std::optional<std::string>>> result;
    for (auto it = entries_.lower_bound(prefix); it != entries_.end() && StartsWith(it->first, prefix); ++it) {
        result.emplace_back(it->first, it->second);
    }
    return result;
}

}  // namespace distdb
