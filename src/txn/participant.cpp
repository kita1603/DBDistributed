#include "participant.h"

namespace distdb {

std::string TxnParticipant::Prepare(TxnId txn_id, std::vector<std::string> keys, std::vector<std::string> commands) {
    auto existing = pending_.find(txn_id);
    if (existing != pending_.end()) {
        return "";  // already prepared (a retry) - nothing new to check or lock
    }

    for (const auto& key : keys) {
        if (locked_keys_.count(key)) {
            return "key '" + key + "' is locked by another in-flight transaction";
        }
    }

    for (const auto& key : keys) locked_keys_.insert(key);
    pending_[txn_id] = Pending{std::move(keys), std::move(commands)};
    return "";
}

std::string TxnParticipant::Commit(TxnId txn_id, const std::function<void(const std::string&)>& run) {
    auto it = pending_.find(txn_id);
    if (it == pending_.end()) {
        return "commit of unknown or already-resolved transaction " + std::to_string(txn_id);
    }
    Pending pending = std::move(it->second);
    pending_.erase(it);
    ReleaseLocks(pending);

    for (const auto& command : pending.commands) run(command);
    return "";
}

std::string TxnParticipant::Abort(TxnId txn_id) {
    auto it = pending_.find(txn_id);
    if (it == pending_.end()) {
        return "";  // never prepared here, or already resolved - a safe no-op either way
    }
    ReleaseLocks(it->second);
    pending_.erase(it);
    return "";
}

void TxnParticipant::ReleaseLocks(const Pending& pending) {
    for (const auto& key : pending.keys) locked_keys_.erase(key);
}

}  // namespace distdb
