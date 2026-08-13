#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace distdb {

// Globally unique across coordinators as long as each one only ever
// generates its own ids: see EncodeTxnPrepare's caller in raft_main.cpp,
// which packs its own NodeId into the high bits.
using TxnId = uint64_t;

enum class TxnControlType : uint8_t {
    kPrepare = 1,
    kCommit = 2,
    kAbort = 3,
};

// A transaction's participant shards each get sent one PREPARE naming
// only *their own* slice of it: the row keys this shard's replicas need
// to lock, and the exact SQL commands to run once (and only once) the
// transaction actually commits. Commands are deferred rather than run
// immediately - staged in TxnParticipant - so that an ABORT never has to
// undo anything already visible to a reader.
struct TxnPrepare {
    TxnId txn_id = 0;
    std::vector<std::string> keys;
    std::vector<std::string> commands;
};

// Every one of these is itself proposed through the normal
// RaftNode::Propose() path as an opaque ClientRequest command, exactly
// like a plain SQL statement - the only difference is the bytes are
// this module's own encoding instead of SQL text. `command[0] == 0`
// marks that ("Execute" would only ever see byte 0 at the very start of
// a real SQL statement never - a table/column name might contain a NUL
// nowhere the lexer permits, and it certainly can't be the *first*
// byte of a whole statement), so ApplyCallback can always tell which of
// the two a given log entry is without ambiguity.
std::string EncodeTxnPrepare(const TxnPrepare& p);
std::string EncodeTxnCommit(TxnId txn_id);
std::string EncodeTxnAbort(TxnId txn_id);

struct TxnControlCommand {
    TxnControlType type;
    TxnId txn_id = 0;
    std::vector<std::string> keys;      // set only for kPrepare
    std::vector<std::string> commands;  // set only for kPrepare
};

// Returns nullopt if `command` isn't one of this module's own encoded
// control commands at all (i.e. it's a plain SQL statement, which the
// caller should hand to SqlExecutor::Execute as usual).
std::optional<TxnControlCommand> TryDecodeTxnControl(const std::string& command);

}  // namespace distdb
