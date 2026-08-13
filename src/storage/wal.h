#pragma once

#include <cstdint>
#include <fstream>
#include <functional>
#include <string>

namespace distdb {

enum class RecordType : uint8_t {
    kPut = 0,
    kDelete = 1,
};

// Invoked for each record replayed from the WAL, in the order it was
// originally written.
using ReplayCallback = std::function<void(RecordType type, const std::string& key, const std::string& value)>;

// Append-only write-ahead log. Every mutation is durably recorded here
// before being applied to the in-memory memtable, so the memtable can be
// rebuilt by replaying this file after a crash. Each record is checksummed
// so a torn write at the tail (crash mid-append) is detected and discarded
// rather than corrupting recovery.
class WriteAheadLog {
 public:
    explicit WriteAheadLog(std::string path);
    ~WriteAheadLog();

    WriteAheadLog(const WriteAheadLog&) = delete;
    WriteAheadLog& operator=(const WriteAheadLog&) = delete;

    // Opens the log for appending, creating the file if it doesn't exist.
    void Open();

    // Reads every valid record from the start of the file and invokes
    // callback for each, in write order. Stops at the first invalid
    // (missing or checksum-mismatched) record, which is the tail of a
    // write that was interrupted by a crash.
    void Replay(const ReplayCallback& callback);

    void AppendPut(const std::string& key, const std::string& value);
    void AppendDelete(const std::string& key);

    // Truncates the log back to empty. Called after a memtable flush:
    // once its entries are durable in an SSTable, the WAL records that
    // rebuilt them are no longer needed for recovery.
    void Reset();

 private:
    void AppendRecord(RecordType type, const std::string& key, const std::string& value);

    std::string path_;
    std::fstream file_;
};

}  // namespace distdb
