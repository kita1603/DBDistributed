#pragma once

#include <string>

namespace distdb {

// A key can be absent from a given layer (memtable or one SSTable), or
// present there either as a live value or as a tombstone. The distinction
// between kNotFound and kDeleted matters once there are multiple layers:
// kNotFound means "check the next, older layer"; kDeleted means "stop -
// this layer's tombstone shadows anything an older layer might still have
// for this key."
enum class LookupStatus {
    kNotFound,
    kFound,
    kDeleted,
};

struct LookupResult {
    LookupStatus status = LookupStatus::kNotFound;
    std::string value;  // meaningful only when status == kFound
};

}  // namespace distdb
