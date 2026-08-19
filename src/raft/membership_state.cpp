#include "membership_state.h"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <stdexcept>

namespace distdb {

namespace {

// Deliberately a small, private copy of routing.cpp's own peer-spec
// parser/formatter rather than a shared dependency on it: this file's
// format is this node's own persisted bookkeeping, not the hand-edited
// shard routing table, even though the two happen to reuse the same
// "id=host:port,..." syntax for a human-readable file.
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

std::string FormatPeerSpec(const std::map<NodeId, PeerAddress>& peers) {
    std::string out;
    bool first = true;
    for (const auto& [id, addr] : peers) {
        if (!first) out += ',';
        first = false;
        out += std::to_string(id) + "=" + addr.host + ":" + std::to_string(addr.port);
    }
    return out;
}

}  // namespace

MembershipState::MembershipState(std::string path) : path_(std::move(path)) {}

std::string MembershipState::PathFor(const std::string& state_dir, NodeId id) {
    return state_dir + "/raft_membership_" + std::to_string(id) + ".txt";
}

void MembershipState::Load() {
    std::ifstream file(path_);
    if (!file.is_open()) return;  // never run before - defaults are correct
    has_file_ = true;

    int self_flag = 1;
    file >> self_flag;
    self_is_member_ = self_flag != 0;

    std::string spec;
    file.ignore();  // the newline left after `>>`
    std::getline(file, spec);
    base_peers_ = ParsePeerSpec(spec);

    std::string own_spec;
    std::getline(file, own_spec);
    size_t colon = own_spec.find(':');
    if (colon != std::string::npos) {
        own_address_ = {own_spec.substr(0, colon), static_cast<uint16_t>(std::stoul(own_spec.substr(colon + 1)))};
    }
}

void MembershipState::Set(std::map<NodeId, PeerAddress> peers, bool self_is_member, PeerAddress own_address) {
    base_peers_ = std::move(peers);
    self_is_member_ = self_is_member;
    own_address_ = std::move(own_address);
    has_file_ = true;

    // Write to a temp file and rename over the real one - the same
    // crash-safety trick PersistentState::Set()/RaftLog::Save() use.
    std::string tmp_path = path_ + ".tmp";
    {
        std::ofstream file(tmp_path, std::ios::trunc);
        file << (self_is_member_ ? 1 : 0) << "\n"
             << FormatPeerSpec(base_peers_) << "\n"
             << own_address_.host << ":" << own_address_.port << "\n";
    }
    std::filesystem::rename(tmp_path, path_);
}

}  // namespace distdb
