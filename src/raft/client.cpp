#include "client.h"

#include <stdexcept>

#include "transport.h"

namespace distdb {

namespace {
constexpr int kMaxHops = 4;
}  // namespace

ClientResponse SendClientRequest(const std::map<NodeId, PeerAddress>& cluster_peers, const std::string& command,
                                  int timeout_ms) {
    if (cluster_peers.empty()) {
        return {false, 0, true, 0, "no peers known for this cluster"};
    }

    NodeId target = cluster_peers.begin()->first;  // arbitrary starting guess
    std::string encoded = EncodeClientRequest({command});

    for (int hop = 0; hop < kMaxHops; hop++) {
        auto it = cluster_peers.find(target);
        if (it == cluster_peers.end()) {
            return {false, 0, true, target, "unknown node id " + std::to_string(target)};
        }

        std::string response;
        try {
            response = RaftTransport::SendRequest(it->second.host, it->second.port, encoded, timeout_ms);
        } catch (...) {
            return {false, 0, true, target, "could not reach node " + std::to_string(target)};
        }

        ClientResponse resp = DecodeClientResponse(response);
        if (resp.success || !resp.not_leader) {
            return resp;  // done - either it committed, or it failed for a reason other than "wrong node"
        }
        if (resp.leader_hint == 0 || resp.leader_hint == target) {
            return resp;  // that node has no better lead than the one we just tried - give up
        }
        target = resp.leader_hint;  // it pointed us further - follow it, up to kMaxHops
    }
    return {false, 0, true, target, "too many redirects while locating the leader"};
}

std::string SendReadRequest(const std::map<NodeId, PeerAddress>& cluster_peers, const std::string& query,
                             int timeout_ms) {
    std::string encoded = EncodeReadRequest({query});
    std::string last_error = "no peers known for this cluster";

    for (const auto& [id, addr] : cluster_peers) {
        (void)id;
        std::string response;
        try {
            response = RaftTransport::SendRequest(addr.host, addr.port, encoded, timeout_ms);
        } catch (const std::exception& e) {
            last_error = e.what();
            continue;  // try the next peer
        }

        ReadResponse resp = DecodeReadResponse(response);
        if (resp.error) throw std::runtime_error(resp.result);
        return resp.result;
    }
    throw std::runtime_error("could not reach any node in this cluster: " + last_error);
}

std::map<NodeId, PeerAddress> DiscoverCluster(const std::string& host, uint16_t port, int timeout_ms) {
    std::string encoded = EncodeDescribeClusterRequest({});
    std::string response = RaftTransport::SendRequest(host, port, encoded, timeout_ms);  // throws on failure

    DescribeClusterResponse resp = DecodeDescribeClusterResponse(response);
    std::map<NodeId, PeerAddress> peers = resp.peers;
    peers[resp.id] = resp.own_address;  // the seed node itself, self-excluded from its own peers_
    return peers;
}

}  // namespace distdb
