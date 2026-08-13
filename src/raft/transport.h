#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <thread>

namespace distdb {

// One-shot request/response RPC transport over TCP: each call opens a
// fresh connection, writes one length-prefixed message, reads one
// length-prefixed response, and closes. That's simpler and easier to
// reason about than multiplexing many calls over a persistent
// connection per peer, at the cost of a new TCP handshake per RPC - an
// acceptable trade at this project's scale (a production Raft keeps a
// connection open per peer instead).
class RaftTransport {
 public:
    using Handler = std::function<std::string(const std::string& request_body)>;

    explicit RaftTransport(uint16_t listen_port);
    ~RaftTransport();

    RaftTransport(const RaftTransport&) = delete;
    RaftTransport& operator=(const RaftTransport&) = delete;

    // Starts a background thread that accepts connections and spawns a
    // detached thread per connection to run `handler` on it. One thread
    // per connection matters now that not every request is guaranteed
    // to be fast: a ClientRequest can block inside `handler` for up to
    // its caller's timeout (seconds) waiting on a Raft commit, and that
    // must not stall this node's ability to accept other incoming RPCs
    // (e.g. an AppendEntries/RequestVote from a peer) in the meantime.
    void Serve(Handler handler);

    // Sends `request_body` to host:port and returns the peer's response.
    // Throws std::runtime_error if connecting, sending, or receiving
    // fails or exceeds `timeout_ms` - the caller treats that the same as
    // "peer unreachable right now", which is routine in a Raft cluster
    // (a follower can be down or slow) rather than exceptional.
    static std::string SendRequest(const std::string& host, uint16_t port, const std::string& request_body,
                                    int timeout_ms);

 private:
    void AcceptLoop(Handler handler);

    uint16_t listen_port_;
    // Holds the platform socket handle (SOCKET on Windows, int on POSIX)
    // without pulling winsock2.h/sys/socket.h into this header.
    intptr_t listen_socket_;
    std::thread accept_thread_;
    bool stop_ = false;
};

}  // namespace distdb
