#include "transport.h"

#include <cstring>
#include <mutex>
#include <stdexcept>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace distdb {

namespace {

#ifdef _WIN32
using NativeSocket = SOCKET;
constexpr NativeSocket kInvalidSocket = INVALID_SOCKET;
void CloseNative(NativeSocket s) { closesocket(s); }
#else
using NativeSocket = int;
constexpr NativeSocket kInvalidSocket = -1;
void CloseNative(NativeSocket s) { close(s); }
#endif

void EnsureSocketsInitialized() {
#ifdef _WIN32
    static std::once_flag once;
    std::call_once(once, [] {
        WSADATA data;
        WSAStartup(MAKEWORD(2, 2), &data);
    });
#endif
}

void RecvExact(NativeSocket sock, char* out, size_t len) {
    size_t received = 0;
    while (received < len) {
        int n = recv(sock, out + received, static_cast<int>(len - received), 0);
        if (n <= 0) throw std::runtime_error("connection closed or timed out while reading");
        received += static_cast<size_t>(n);
    }
}

void SendExact(NativeSocket sock, const char* data, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        int n = send(sock, data + sent, static_cast<int>(len - sent), 0);
        if (n <= 0) throw std::runtime_error("connection closed or timed out while writing");
        sent += static_cast<size_t>(n);
    }
}

std::string ReadFramedMessage(NativeSocket sock) {
    uint32_t len = 0;
    RecvExact(sock, reinterpret_cast<char*>(&len), sizeof(len));
    std::string body(len, '\0');
    if (len > 0) RecvExact(sock, body.data(), len);
    return body;
}

void WriteFramedMessage(NativeSocket sock, const std::string& body) {
    uint32_t len = static_cast<uint32_t>(body.size());
    SendExact(sock, reinterpret_cast<const char*>(&len), sizeof(len));
    if (len > 0) SendExact(sock, body.data(), len);
}

void SetTimeout(NativeSocket sock, int timeout_ms) {
#ifdef _WIN32
    DWORD timeout = static_cast<DWORD>(timeout_ms);
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));
#else
    timeval timeout{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
#endif
}

void SetBlocking(NativeSocket sock, bool blocking) {
#ifdef _WIN32
    u_long mode = blocking ? 0 : 1;
    ioctlsocket(sock, FIONBIO, &mode);
#else
    int flags = fcntl(sock, F_GETFL, 0);
    fcntl(sock, F_SETFL, blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK));
#endif
}

// SO_RCVTIMEO/SO_SNDTIMEO (set by SetTimeout, above) bound send()/recv()
// but not connect() itself on every platform - a plain blocking
// connect() to a host that never responds (packets silently dropped by
// a firewall, for instance, rather than actively refused) can hang for
// the operating system's own default TCP connect timeout, which is
// commonly tens of seconds. That's invisible on localhost, where a dead
// port refuses the connection immediately, but becomes a real, visible
// hang once peers are reachable over an actual network instead. Making
// the socket non-blocking during connect() and bounding it with
// select() gives connect() a real, enforced timeout on both platforms.
bool ConnectWithTimeout(NativeSocket sock, const sockaddr* addr, size_t addr_len, int timeout_ms) {
    SetBlocking(sock, false);

    int rc = connect(sock, addr, static_cast<int>(addr_len));
    if (rc == 0) {
        SetBlocking(sock, true);
        return true;  // connected immediately (e.g. localhost)
    }

#ifdef _WIN32
    if (WSAGetLastError() != WSAEWOULDBLOCK) return false;
#else
    if (errno != EINPROGRESS) return false;
#endif

    fd_set write_set;
    FD_ZERO(&write_set);
    FD_SET(sock, &write_set);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};

    int ready = select(static_cast<int>(sock) + 1, nullptr, &write_set, nullptr, &tv);
    if (ready <= 0) return false;  // timed out, or select() itself failed

    int so_error = 0;
    socklen_t err_len = sizeof(so_error);
    getsockopt(sock, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&so_error), &err_len);
    SetBlocking(sock, true);
    return so_error == 0;
}

}  // namespace

RaftTransport::RaftTransport(uint16_t listen_port) : listen_port_(listen_port), listen_socket_(-1) {
    EnsureSocketsInitialized();

    NativeSocket sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == kInvalidSocket) throw std::runtime_error("failed to create listening socket");

    int reuse = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&reuse), sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(listen_port);

    if (bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        CloseNative(sock);
        throw std::runtime_error("failed to bind to port " + std::to_string(listen_port));
    }
    if (listen(sock, 16) != 0) {
        CloseNative(sock);
        throw std::runtime_error("failed to listen on port " + std::to_string(listen_port));
    }

    listen_socket_ = static_cast<intptr_t>(sock);
}

RaftTransport::~RaftTransport() {
    stop_ = true;
    if (listen_socket_ != -1) CloseNative(static_cast<NativeSocket>(listen_socket_));
    if (accept_thread_.joinable()) accept_thread_.join();
}

void RaftTransport::Serve(Handler handler) {
    accept_thread_ = std::thread([this, handler] { AcceptLoop(handler); });
}

void RaftTransport::AcceptLoop(Handler handler) {
    NativeSocket listener = static_cast<NativeSocket>(listen_socket_);
    while (!stop_) {
        sockaddr_in peer_addr{};
#ifdef _WIN32
        int addr_len = sizeof(peer_addr);
#else
        socklen_t addr_len = sizeof(peer_addr);
#endif
        NativeSocket conn = accept(listener, reinterpret_cast<sockaddr*>(&peer_addr), &addr_len);
        if (conn == kInvalidSocket) {
            if (stop_) break;
            continue;
        }

        // One thread per connection: a ClientRequest can legitimately
        // block inside `handler` for seconds waiting on a Raft commit,
        // and that must not stop this loop from accepting the next
        // (possibly time-sensitive) incoming RPC in the meantime.
        std::thread([conn, handler] {
            try {
                std::string request = ReadFramedMessage(conn);
                std::string response = handler(request);
                WriteFramedMessage(conn, response);
            } catch (...) {
                // A malformed message or a peer that drops the
                // connection mid-RPC only affects this one connection.
            }
            CloseNative(conn);
        }).detach();
    }
}

std::string RaftTransport::SendRequest(const std::string& host, uint16_t port, const std::string& request_body,
                                        int timeout_ms) {
    EnsureSocketsInitialized();

    NativeSocket sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == kInvalidSocket) throw std::runtime_error("failed to create client socket");
    SetTimeout(sock, timeout_ms);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) != 1) {
        CloseNative(sock);
        throw std::runtime_error("invalid host address: " + host);
    }

    if (!ConnectWithTimeout(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr), timeout_ms)) {
        CloseNative(sock);
        throw std::runtime_error("failed to connect to " + host + ":" + std::to_string(port) + " within " +
                                  std::to_string(timeout_ms) + "ms");
    }

    try {
        WriteFramedMessage(sock, request_body);
        std::string response = ReadFramedMessage(sock);
        CloseNative(sock);
        return response;
    } catch (...) {
        CloseNative(sock);
        throw;
    }
}

}  // namespace distdb
