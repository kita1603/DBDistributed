#include "message.h"

#include <cstring>
#include <stdexcept>

namespace distdb {

namespace {

void AppendU8(std::string& out, uint8_t v) { out.push_back(static_cast<char>(v)); }
void AppendU32(std::string& out, uint32_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendU64(std::string& out, uint64_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendString(std::string& out, const std::string& s) {
    AppendU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}

// Small cursor-based reader so each Decode* function stays a flat list
// of reads instead of hand-tracking an offset everywhere.
class Reader {
 public:
    explicit Reader(const std::string& buf) : buf_(buf) {}

    uint8_t ReadU8() {
        Check(1);
        uint8_t v = static_cast<uint8_t>(buf_[pos_]);
        pos_ += 1;
        return v;
    }
    uint32_t ReadU32() {
        Check(sizeof(uint32_t));
        uint32_t v;
        std::memcpy(&v, buf_.data() + pos_, sizeof(v));
        pos_ += sizeof(v);
        return v;
    }
    uint64_t ReadU64() {
        Check(sizeof(uint64_t));
        uint64_t v;
        std::memcpy(&v, buf_.data() + pos_, sizeof(v));
        pos_ += sizeof(v);
        return v;
    }
    std::string ReadString() {
        uint32_t len = ReadU32();
        Check(len);
        std::string s = buf_.substr(pos_, len);
        pos_ += len;
        return s;
    }

 private:
    void Check(size_t n) const {
        if (pos_ + n > buf_.size()) throw std::runtime_error("corrupt or truncated raft message");
    }

    const std::string& buf_;
    size_t pos_ = 0;
};

}  // namespace

MessageType PeekMessageType(const std::string& body) {
    if (body.empty()) throw std::runtime_error("empty raft message");
    return static_cast<MessageType>(static_cast<uint8_t>(body[0]));
}

std::string EncodeRequestVoteRequest(const RequestVoteRequest& req) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kRequestVoteRequest));
    AppendU64(out, req.term);
    AppendU32(out, req.candidate_id);
    AppendU64(out, req.last_log_index);
    AppendU64(out, req.last_log_term);
    return out;
}

RequestVoteRequest DecodeRequestVoteRequest(const std::string& body) {
    Reader r(body);
    r.ReadU8();  // type tag
    RequestVoteRequest req;
    req.term = r.ReadU64();
    req.candidate_id = r.ReadU32();
    req.last_log_index = r.ReadU64();
    req.last_log_term = r.ReadU64();
    return req;
}

std::string EncodeRequestVoteResponse(const RequestVoteResponse& resp) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kRequestVoteResponse));
    AppendU64(out, resp.term);
    AppendU8(out, resp.vote_granted ? 1 : 0);
    return out;
}

RequestVoteResponse DecodeRequestVoteResponse(const std::string& body) {
    Reader r(body);
    r.ReadU8();
    RequestVoteResponse resp;
    resp.term = r.ReadU64();
    resp.vote_granted = r.ReadU8() != 0;
    return resp;
}

std::string EncodeAppendEntriesRequest(const AppendEntriesRequest& req) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kAppendEntriesRequest));
    AppendU64(out, req.term);
    AppendU32(out, req.leader_id);
    AppendU64(out, req.prev_log_index);
    AppendU64(out, req.prev_log_term);
    AppendU32(out, static_cast<uint32_t>(req.entries.size()));
    for (const auto& e : req.entries) {
        AppendU64(out, e.index);
        AppendU64(out, e.term);
        AppendString(out, e.command);
    }
    AppendU64(out, req.leader_commit);
    return out;
}

AppendEntriesRequest DecodeAppendEntriesRequest(const std::string& body) {
    Reader r(body);
    r.ReadU8();
    AppendEntriesRequest req;
    req.term = r.ReadU64();
    req.leader_id = r.ReadU32();
    req.prev_log_index = r.ReadU64();
    req.prev_log_term = r.ReadU64();
    uint32_t count = r.ReadU32();
    req.entries.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        LogEntry e;
        e.index = r.ReadU64();
        e.term = r.ReadU64();
        e.command = r.ReadString();
        req.entries.push_back(std::move(e));
    }
    req.leader_commit = r.ReadU64();
    return req;
}

std::string EncodeAppendEntriesResponse(const AppendEntriesResponse& resp) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kAppendEntriesResponse));
    AppendU64(out, resp.term);
    AppendU8(out, resp.success ? 1 : 0);
    AppendU64(out, resp.match_index);
    return out;
}

AppendEntriesResponse DecodeAppendEntriesResponse(const std::string& body) {
    Reader r(body);
    r.ReadU8();
    AppendEntriesResponse resp;
    resp.term = r.ReadU64();
    resp.success = r.ReadU8() != 0;
    resp.match_index = r.ReadU64();
    return resp;
}

std::string EncodeClientRequest(const ClientRequest& req) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kClientRequest));
    AppendString(out, req.command);
    return out;
}

ClientRequest DecodeClientRequest(const std::string& body) {
    Reader r(body);
    r.ReadU8();
    ClientRequest req;
    req.command = r.ReadString();
    return req;
}

std::string EncodeClientResponse(const ClientResponse& resp) {
    std::string out;
    AppendU8(out, static_cast<uint8_t>(MessageType::kClientResponse));
    AppendU8(out, resp.success ? 1 : 0);
    AppendU64(out, resp.index);
    AppendU8(out, resp.not_leader ? 1 : 0);
    AppendU32(out, resp.leader_hint);
    AppendString(out, resp.error);
    return out;
}

ClientResponse DecodeClientResponse(const std::string& body) {
    Reader r(body);
    r.ReadU8();
    ClientResponse resp;
    resp.success = r.ReadU8() != 0;
    resp.index = r.ReadU64();
    resp.not_leader = r.ReadU8() != 0;
    resp.leader_hint = r.ReadU32();
    resp.error = r.ReadString();
    return resp;
}

}  // namespace distdb
