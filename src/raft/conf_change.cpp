#include "conf_change.h"

#include <cstring>
#include <stdexcept>

namespace distdb {

namespace {

// Deliberately a small, private copy of message.cpp's/txn.cpp's own
// length-prefix pattern rather than a shared dependency on either: this
// module's wire format is an internal implementation detail of how
// raft_main.cpp tags its own log entries, not a Raft RPC and not a txn
// control command, so there's no real coupling to give up by keeping it
// separate.
void AppendU8(std::string& out, uint8_t v) { out.push_back(static_cast<char>(v)); }
void AppendU16(std::string& out, uint16_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendU32(std::string& out, uint32_t v) { out.append(reinterpret_cast<const char*>(&v), sizeof(v)); }
void AppendString(std::string& out, const std::string& s) {
    AppendU32(out, static_cast<uint32_t>(s.size()));
    out.append(s);
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
    uint16_t ReadU16() {
        Check(sizeof(uint16_t));
        uint16_t v;
        std::memcpy(&v, buf_.data() + pos_, sizeof(v));
        pos_ += sizeof(v);
        return v;
    }
    uint32_t ReadU32() {
        Check(sizeof(uint32_t));
        uint32_t v;
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
        if (pos_ + n > buf_.size()) throw std::runtime_error("corrupt or truncated conf-change command");
    }

    const std::string& buf_;
    size_t pos_ = 0;
};

}  // namespace

std::string EncodeConfChange(const ConfChangeCommand& c) {
    std::string out;
    AppendU8(out, 1);  // conf-change tag - see conf_change.h's doc comment
    AppendU8(out, static_cast<uint8_t>(c.type));
    AppendU32(out, c.server_id);
    AppendString(out, c.host);
    AppendU16(out, c.port);
    return out;
}

std::optional<ConfChangeCommand> TryDecodeConfChange(const std::string& command) {
    if (command.empty() || static_cast<uint8_t>(command[0]) != 1) return std::nullopt;

    Reader r(command);
    r.ReadU8();  // tag
    ConfChangeCommand out;
    out.type = static_cast<ConfChangeType>(r.ReadU8());
    out.server_id = r.ReadU32();
    out.host = r.ReadString();
    out.port = r.ReadU16();
    return out;
}

}  // namespace distdb
