#pragma once

#include <cstdint>
#include <optional>
#include <string>

#include "types.h"

namespace distdb {

enum class ConfChangeType : uint8_t {
    kAddServer = 1,
    kRemoveServer = 2,
};

// A cluster membership change, proposed through the normal
// RaftNode::Propose() path as an opaque command - exactly like a SQL
// statement or a txn control command, just tagged differently. `command[0]
// == 1` marks that (txn.h already claims byte 0 for its own control
// commands; no real SQL statement or txn command can start with either),
// so RaftNode can always tell a membership change apart from anything else
// in the log without ambiguity, and ApplyCommitted() can keep it from ever
// reaching apply_callback_ - membership is a pure Raft concept, unrelated
// to the SQL/txn state machine.
struct ConfChangeCommand {
    ConfChangeType type;
    NodeId server_id = 0;
    std::string host;   // only meaningful for kAddServer
    uint16_t port = 0;   // only meaningful for kAddServer
};

std::string EncodeConfChange(const ConfChangeCommand& c);

// Returns nullopt if `command` isn't one of this module's own encoded
// commands at all.
std::optional<ConfChangeCommand> TryDecodeConfChange(const std::string& command);

}  // namespace distdb
