#include "routing.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace distdb {

namespace {

// Same peer-list syntax already used for a single Raft group's peers
// (see ParsePeers in raft_main.cpp) - duplicated here rather than shared
// because it's a handful of lines and pulling in raft_main.cpp's
// anonymous-namespace helper isn't possible anyway (it isn't exported).
std::map<NodeId, PeerAddress> ParsePeerSpec(const std::string& spec) {
    std::map<NodeId, PeerAddress> peers;
    std::istringstream stream(spec);
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        if (entry.empty()) continue;
        size_t eq = entry.find('=');
        size_t colon = entry.find(':', eq == std::string::npos ? 0 : eq);
        if (eq == std::string::npos || colon == std::string::npos) {
            throw std::runtime_error("bad peer spec: " + entry);
        }
        auto id = static_cast<NodeId>(std::stoul(entry.substr(0, eq)));
        std::string host = entry.substr(eq + 1, colon - eq - 1);
        auto port = static_cast<uint16_t>(std::stoul(entry.substr(colon + 1)));
        peers[id] = {host, port};
    }
    return peers;
}

}  // namespace

void RoutingTable::LoadFromFile(const std::string& path) {
    std::ifstream file(path);
    if (!file.is_open()) throw std::runtime_error("failed to open routing table: " + path);

    shards_.clear();
    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;

        std::istringstream iss(line);
        ShardRange shard;
        std::string start_token, end_token, peers_token;
        if (!(iss >> shard.id >> start_token >> end_token >> peers_token)) continue;  // blank/malformed line

        shard.start_key = (start_token == "*") ? "" : start_token;
        shard.end_key = (end_token == "*") ? "" : end_token;
        shard.peers = ParsePeerSpec(peers_token);
        shards_.push_back(std::move(shard));
    }

    if (shards_.empty()) throw std::runtime_error("routing table is empty: " + path);

    std::sort(shards_.begin(), shards_.end(),
              [](const ShardRange& a, const ShardRange& b) { return a.start_key < b.start_key; });
}

const ShardRange& RoutingTable::ShardFor(const std::string& key) const {
    // Find the first shard whose start_key is strictly past `key`, then
    // step back one - that's the last shard whose start_key <= key,
    // which (given well-formed, contiguous ranges) is the one covering it.
    auto it = std::upper_bound(shards_.begin(), shards_.end(), key,
                                [](const std::string& k, const ShardRange& shard) { return k < shard.start_key; });
    if (it == shards_.begin()) {
        throw std::runtime_error("no shard covers key: " + key);
    }
    --it;
    return *it;
}

const ShardRange& RoutingTable::Shard(ShardId id) const {
    for (const auto& shard : shards_) {
        if (shard.id == id) return shard;
    }
    throw std::runtime_error("unknown shard id: " + std::to_string(id));
}

std::vector<ShardId> RoutingTable::AllShardIds() const {
    std::vector<ShardId> ids;
    ids.reserve(shards_.size());
    for (const auto& shard : shards_) ids.push_back(shard.id);
    return ids;
}

}  // namespace distdb
