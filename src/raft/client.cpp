#include "client.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <mutex>
#include <stdexcept>
#include <utility>

#include "transport.h"

namespace distdb {

namespace {
constexpr int kMaxHops = 4;

// How many *consecutive* RPC failures against one (host, port) trip its
// circuit, and how long it then stays tripped before being tried again -
// small numbers, same convention as this project's other thresholds
// (kMemtableFlushThreshold, kCompactionThreshold), chosen so the effect is
// easy to observe by hand rather than tuned for a real workload.
constexpr int kFailureThreshold = 2;
constexpr auto kCooldown = std::chrono::milliseconds(3000);

// Per-process, per-endpoint memory of "did the last few RPCs to this
// address fail" - shared by SendClientRequest and SendReadRequest below so
// both stop paying a full RPC timeout against a peer either of them has
// already found unreachable. Deliberately host/port-keyed rather than
// NodeId-keyed: a NodeId is only unique within one shard's cluster_peers
// map, but a process (raftui, or raft_main.cpp forwarding to another
// shard) may deal with several different clusters at once, and the same
// physical address is what actually goes offline.
//
// This is a plain failure-count-with-cooldown breaker, not a full closed/
// open/half-open state machine: once `open_until` passes, the address is
// simply eligible to be tried again - a success there resets the count,
// a failure re-trips it. Simpler, and sufficient for a client that isn't
// under sustained load.
class CircuitBreaker {
 public:
    static CircuitBreaker& Instance() {
        static CircuitBreaker instance;
        return instance;
    }

    bool IsOpen(const std::string& host, uint16_t port) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = health_.find({host, port});
        return it != health_.end() && std::chrono::steady_clock::now() < it->second.open_until;
    }

    void RecordSuccess(const std::string& host, uint16_t port) {
        std::lock_guard<std::mutex> lock(mutex_);
        health_.erase({host, port});
    }

    void RecordFailure(const std::string& host, uint16_t port) {
        std::lock_guard<std::mutex> lock(mutex_);
        Health& h = health_[{host, port}];
        if (++h.consecutive_failures >= kFailureThreshold) {
            h.open_until = std::chrono::steady_clock::now() + kCooldown;
        }
    }

 private:
    struct Health {
        int consecutive_failures = 0;
        std::chrono::steady_clock::time_point open_until{};
    };

    std::mutex mutex_;
    std::map<std::pair<std::string, uint16_t>, Health> health_;
};

}  // namespace

ClientResponse SendClientRequest(const std::map<NodeId, PeerAddress>& cluster_peers, const std::string& command,
                                  int timeout_ms) {
    if (cluster_peers.empty()) {
        return {false, 0, true, 0, "no peers known for this cluster"};
    }

    // Arbitrary starting guess - but prefer one the breaker doesn't
    // already consider tripped, so a node already known to be unreachable
    // isn't picked first just because it happens to have the lowest id.
    NodeId target = cluster_peers.begin()->first;
    for (const auto& [id, addr] : cluster_peers) {
        if (!CircuitBreaker::Instance().IsOpen(addr.host, addr.port)) {
            target = id;
            break;
        }
    }

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
            CircuitBreaker::Instance().RecordFailure(it->second.host, it->second.port);
            // A dead connection isn't this node's fault to redirect
            // around the way a stale leader_hint is (below) - it means
            // this *guess* was wrong, not that the cluster has no leader.
            // Try one other peer the breaker doesn't already consider
            // tripped, the same way SendReadRequest tries every peer in
            // turn, rather than giving up on the very first unlucky guess.
            auto retry = std::find_if(cluster_peers.begin(), cluster_peers.end(), [&](const auto& kv) {
                return kv.first != target && !CircuitBreaker::Instance().IsOpen(kv.second.host, kv.second.port);
            });
            if (retry == cluster_peers.end()) {
                return {false, 0, true, target, "could not reach node " + std::to_string(target)};
            }
            target = retry->first;
            continue;
        }
        CircuitBreaker::Instance().RecordSuccess(it->second.host, it->second.port);

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

    // Two passes: peers the breaker doesn't consider tripped first (skips
    // a known-dead node's RPC timeout entirely), then whatever's left - so
    // a cluster that's genuinely all unreachable right now still gets one
    // real attempt per peer instead of failing without ever trying
    // anyone, and a peer that's recovered past its cooldown gets a chance
    // to prove it in the same call rather than waiting for the next one.
    for (bool pass_wants_tripped : {false, true}) {
        for (const auto& [id, addr] : cluster_peers) {
            (void)id;
            bool tripped = CircuitBreaker::Instance().IsOpen(addr.host, addr.port);
            if (tripped != pass_wants_tripped) continue;  // pass 1: only !tripped; pass 2: only tripped

            std::string response;
            try {
                response = RaftTransport::SendRequest(addr.host, addr.port, encoded, timeout_ms);
            } catch (const std::exception& e) {
                CircuitBreaker::Instance().RecordFailure(addr.host, addr.port);
                last_error = e.what();
                continue;  // try the next peer
            }
            CircuitBreaker::Instance().RecordSuccess(addr.host, addr.port);

            ReadResponse resp = DecodeReadResponse(response);
            if (resp.error) throw std::runtime_error(resp.result);
            return resp.result;
        }
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
