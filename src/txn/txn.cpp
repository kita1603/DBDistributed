#include "txn.h"

#include <cstring>
#include <stdexcept>

namespace distdb {

namespace {

// Deliberately a small, private copy of message.cpp's own length-prefix
// pattern rather than a shared dependency on it: this module's "wire
// format" is an internal implementation detail of how raft_main.cpp
// tags its own log entries, not a Raft RPC, so there's no real coupling
// to give up by keeping them separate.
void AppendU8(std::string& out, uint8_t v) { out.push_back(static_cast<char>(v)); }
void AppendU32(std::string& out, uint32_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendU64(std::string& out, uint64_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendString(std::string& out, const std::string& s) {
    AppendU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
}
void AppendStringVector(std::string& out, const std::vector<std::string>& v) {
    AppendU32(out, static_cast<uint32_t>(v.size()));
    for (const auto& s : v) AppendString(out, s);
}

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
    std::vector<std::string> ReadStringVector() {
        uint32_t count = ReadU32();
        std::vector<std::string> v;
        v.reserve(count);
        for (uint32_t i = 0; i < count; i++) v.push_back(ReadString());
        return v;
    }

 private:
    void Check(size_t n) const {
        if (pos_ + n > buf_.size()) throw std::runtime_error("corrupt or truncated txn control command");
    }

    const std::string& buf_;
    size_t pos_ = 0;
};

}  // namespace

std::string EncodeTxnPrepare(const TxnPrepare& p) {
    std::string out;
    AppendU8(out, 0);  // control-command tag - see txn.h's doc comment
    AppendU8(out, static_cast<uint8_t>(TxnControlType::kPrepare));
    AppendU64(out, p.txn_id);
    AppendStringVector(out, p.keys);
    AppendStringVector(out, p.commands);
    return out;
}

std::string EncodeTxnCommit(TxnId txn_id) {
    std::string out;
    AppendU8(out, 0);
    AppendU8(out, static_cast<uint8_t>(TxnControlType::kCommit));
    AppendU64(out, txn_id);
    return out;
}

std::string EncodeTxnAbort(TxnId txn_id) {
    std::string out;
    AppendU8(out, 0);
    AppendU8(out, static_cast<uint8_t>(TxnControlType::kAbort));
    AppendU64(out, txn_id);
    return out;
}

std::optional<TxnControlCommand> TryDecodeTxnControl(const std::string& command) {
    if (command.empty() || static_cast<uint8_t>(command[0]) != 0) return std::nullopt;

    Reader r(command);
    r.ReadU8();  // tag
    TxnControlCommand out;
    out.type = static_cast<TxnControlType>(r.ReadU8());
    out.txn_id = r.ReadU64();
    if (out.type == TxnControlType::kPrepare) {
        out.keys = r.ReadStringVector();
        out.commands = r.ReadStringVector();
    }
    return out;
}

}  // namespace distdb
