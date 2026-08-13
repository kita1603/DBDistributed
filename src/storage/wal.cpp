#include "wal.h"

#include <stdexcept>

#include "crc32.h"

namespace distdb {

namespace {

// Fixed-size header written before every record. key_len/value_len tell us
// how many bytes follow; value_len is always 0 for kDelete records since
// deletes only need a key (tombstone).
struct RecordHeader {
    uint32_t crc;
    uint32_t key_len;
    uint32_t value_len;
    RecordType type;
};

// Checksum covers type+key+value so both corruption and a torn write of
// any field are caught.
uint32_t ChecksumRecord(RecordType type, const std::string& key, const std::string& value) {
    std::string buf;
    buf.reserve(1 + key.size() + value.size());
    buf.push_back(static_cast<char>(type));
    buf.append(key);
    buf.append(value);
    return Crc32(buf.data(), buf.size());
}

}  // namespace

WriteAheadLog::WriteAheadLog(std::string path) : path_(std::move(path)) {}

WriteAheadLog::~WriteAheadLog() {
    if (file_.is_open()) file_.close();
}

void WriteAheadLog::Open() {
    // Try opening an existing file first; std::ios::out alone would
    // truncate it, which we must not do since it may hold prior data.
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        // File doesn't exist yet - create it, then reopen for read+write.
        std::ofstream create(path_, std::ios::out | std::ios::binary);
        create.close();
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    }
    if (!file_.is_open()) {
        throw std::runtime_error("failed to open WAL file: " + path_);
    }
    file_.seekp(0, std::ios::end);
}

void WriteAheadLog::Replay(const ReplayCallback& callback) {
    file_.clear();
    file_.seekg(0, std::ios::beg);

    while (true) {
        RecordHeader header{};
        file_.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (file_.gcount() != sizeof(header)) break;  // EOF, or a torn header

        std::string key(header.key_len, '\0');
        if (header.key_len > 0) {
            file_.read(key.data(), header.key_len);
            if (file_.gcount() != static_cast<std::streamsize>(header.key_len)) break;
        }

        std::string value;
        if (header.type == RecordType::kPut && header.value_len > 0) {
            value.resize(header.value_len);
            file_.read(value.data(), header.value_len);
            if (file_.gcount() != static_cast<std::streamsize>(header.value_len)) break;
        }

        if (ChecksumRecord(header.type, key, value) != header.crc) {
            // Torn write at the tail (crash mid-append) - everything
            // before this point is still valid and already delivered.
            break;
        }

        callback(header.type, key, value);
    }

    // Position back at the end for subsequent appends.
    file_.clear();
    file_.seekp(0, std::ios::end);
}

void WriteAheadLog::Reset() {
    file_.close();
    {
        std::ofstream truncate(path_, std::ios::out | std::ios::binary | std::ios::trunc);
    }
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        throw std::runtime_error("failed to reopen WAL after reset: " + path_);
    }
    file_.seekp(0, std::ios::end);
}

void WriteAheadLog::AppendPut(const std::string& key, const std::string& value) {
    AppendRecord(RecordType::kPut, key, value);
}

void WriteAheadLog::AppendDelete(const std::string& key) {
    AppendRecord(RecordType::kDelete, key, "");
}

void WriteAheadLog::AppendRecord(RecordType type, const std::string& key, const std::string& value) {
    RecordHeader header{};
    header.crc = ChecksumRecord(type, key, value);
    header.key_len = static_cast<uint32_t>(key.size());
    header.value_len = static_cast<uint32_t>(value.size());
    header.type = type;

    file_.write(reinterpret_cast<const char*>(&header), sizeof(header));
    file_.write(key.data(), static_cast<std::streamsize>(key.size()));
    if (type == RecordType::kPut) {
        file_.write(value.data(), static_cast<std::streamsize>(value.size()));
    }

    // flush() pushes data from the C++ stream buffer to the OS, which is
    // enough to survive a process crash. It does NOT guarantee the OS has
    // written it to physical disk (surviving a power loss) - a real
    // implementation would additionally call fsync()/FlushFileBuffers()
    // on the underlying file descriptor/handle for that guarantee.
    file_.flush();
}

}  // namespace distdb
