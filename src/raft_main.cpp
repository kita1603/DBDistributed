#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "raft/client.h"
#include "raft/conf_change.h"
#include "raft/membership_state.h"
#include "raft/node.h"
#include "shard/routing.h"
#include "sql/ast.h"
#include "sql/executor.h"
#include "sql/lexer.h"
#include "sql/parser.h"
#include "storage/engine.h"
#include "txn/participant.h"
#include "txn/txn.h"

namespace {

// Timeout for a request that has to go over the network to a *different*
// shard's cluster - longer than a within-shard ProposeOrForward's default,
// since it may itself involve that shard following its own leader
// redirects before replying.
constexpr int kCrossShardTimeoutMs = 4000;

// A snapshot is just every key/value pair the local StorageEngine
// currently holds, length-prefixed the same way EncodeRow/DecodeRow
// encode a SQL row in sql/executor.cpp - reused here because the same
// problem (arbitrary bytes, need a self-describing binary format)
// applies to a whole KV dump too.
std::string EncodeSnapshot(const std::vector<std::pair<std::string, std::string>>& kvs) {
    std::string out;
    uint32_t count = static_cast<uint32_t>(kvs.size());
    out.append(reinterpret_cast<const char*>(&count), sizeof(count));
    for (const auto& [key, value] : kvs) {
        uint32_t klen = static_cast<uint32_t>(key.size());
        out.append(reinterpret_cast<const char*>(&klen), sizeof(klen));
        out.append(key);
        uint32_t vlen = static_cast<uint32_t>(value.size());
        out.append(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
        out.append(value);
    }
    return out;
}

std::vector<std::pair<std::string, std::string>> DecodeSnapshot(const std::string& blob) {
    std::vector<std::pair<std::string, std::string>> result;
    size_t pos = 0;
    auto ReadU32 = [&](uint32_t& out) {
        if (pos + sizeof(uint32_t) > blob.size()) throw std::runtime_error("corrupt snapshot");
        std::memcpy(&out, blob.data() + pos, sizeof(uint32_t));
        pos += sizeof(uint32_t);
    };

    uint32_t count = 0;
    ReadU32(count);
    result.reserve(count);
    for (uint32_t i = 0; i < count; i++) {
        uint32_t klen = 0;
        ReadU32(klen);
        std::string key = blob.substr(pos, klen);
        pos += klen;
        uint32_t vlen = 0;
        ReadU32(vlen);
        std::string value = blob.substr(pos, vlen);
        pos += vlen;
        result.emplace_back(std::move(key), std::move(value));
    }
    return result;
}

const char* RoleName(distdb::Role role) {
    switch (role) {
        case distdb::Role::kFollower:
            return "follower";
        case distdb::Role::kCandidate:
            return "candidate";
        case distdb::Role::kLeader:
            return "leader";
    }
    return "?";
}

// Prints how long one dispatched SQL statement took, the moment its scope
// ends - covers every dispatch branch below (CREATE/ALTER TABLE broadcast,
// scatter/gather, single-shard local/remote) uniformly, including their
// several early `continue`s, without a separate print call duplicated at
// each one: declaring one of these right after a statement parses
// successfully is enough, the same way a std::lock_guard releases no
// matter which return/continue/exception unwinds its scope.
class StatementTimer {
 public:
    StatementTimer() : start_(std::chrono::steady_clock::now()) {}
    ~StatementTimer() {
        double ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - start_).count();
        std::cout << "(" << std::fixed << std::setprecision(2) << ms << "ms)\n";
    }

 private:
    std::chrono::steady_clock::time_point start_;
};

void PrintClientResponse(const distdb::ClientResponse& resp) {
    if (resp.success) {
        std::cout << "OK (committed at index " << resp.index << ", via leader node " << resp.leader_hint << ")\n";
        return;
    }
    std::cout << "FAILED";
    if (resp.not_leader && resp.leader_hint != 0) {
        std::cout << " (best known leader is node " << resp.leader_hint << ", but still failed";
        if (!resp.error.empty()) std::cout << ": " << resp.error;
        std::cout << " - retry)";
    } else if (!resp.error.empty()) {
        std::cout << " (" << resp.error << ")";
    }
    std::cout << "\n";
}

void PrintHelp() {
    std::cout << "Enter SQL statements (CREATE TABLE / ALTER TABLE ADD COLUMN / INSERT /\n"
                 "SELECT / UPDATE / DELETE / SHOW TABLES). CREATE TABLE and ALTER TABLE are\n"
                 "broadcast to every shard. INSERT, and any SELECT/UPDATE/DELETE whose WHERE\n"
                 "pins the primary key with '=', is routed to the one shard that owns it\n"
                 "(replicated through Raft the same as a single-shard cluster, forwarding\n"
                 "over the network first if that shard isn't this node's). Anything else (no\n"
                 "WHERE <pk> = ...) is scattered to every shard and the results/counts\n"
                 "gathered back into one answer. SHOW TABLES only ever asks this node's own\n"
                 "shard - every shard's schema copy is already identical.\n"
                 "\n"
                 "  begin              start a transaction: buffers writes instead of running\n"
                 "                     them, until commit/rollback. Only INSERT/UPDATE/DELETE\n"
                 "                     whose WHERE pins the primary key are allowed inside one\n"
                 "                     (no CREATE/ALTER TABLE, no SELECT, no whole-table scatter).\n"
                 "  commit             two-phase commit every buffered statement, atomically\n"
                 "                     across however many shards they touch\n"
                 "  rollback           discard the buffer - nothing was ever sent anywhere\n"
                 "  status             show role / term / current leader hint / shard id\n"
                 "  add-server <id> <host> <port>\n"
                 "                     add a server to this shard's Raft group (start it first\n"
                 "                     with --joining <own-host>)\n"
                 "  remove-server <id> remove a server from this shard's Raft group (the leader\n"
                 "                     itself may be removed - it steps down once it commits)\n"
                 "  help\n"
                 "  exit\n";
}

// Result of proposing the same write to every shard: CREATE TABLE (which
// every shard needs a copy of, since any of them could end up holding
// rows for it) and a table-wide UPDATE/DELETE (which each shard applies
// independently to whatever rows it holds - no cross-shard coordination
// needed, since shards own disjoint rows) both go through this.
struct BroadcastResult {
    size_t ok_count = 0;
    std::vector<std::string> failures;
};

BroadcastResult BroadcastWrite(distdb::RoutingTable& routing, distdb::RaftNode& node, distdb::ShardId my_shard_id,
                                const std::string& command, int timeout_ms) {
    BroadcastResult result;
    for (auto sid : routing.AllShardIds()) {
        distdb::ClientResponse resp;
        if (sid == my_shard_id) {
            resp = node.ProposeOrForward(command);
        } else {
            resp = distdb::SendClientRequest(routing.Shard(sid).peers, command, timeout_ms);
        }
        if (resp.success) {
            result.ok_count++;
        } else {
            result.failures.push_back("shard " + std::to_string(sid) + ": " + resp.error);
        }
    }
    return result;
}

void PrintBroadcastResult(const BroadcastResult& result, const char* verb) {
    if (result.failures.empty()) {
        std::cout << "OK (" << verb << " on " << result.ok_count << " shard(s))\n";
    } else {
        std::cout << "PARTIAL (" << result.ok_count << " shard(s) ok):";
        for (const auto& f : result.failures) std::cout << " [" << f << "]";
        std::cout << "\n";
    }
}

// Merges each shard's independently-formatted SELECT result (a header
// line, then one line per matching row, or "(0 rows)" if none) into one:
// every shard ran the identical query against the same table, so their
// headers agree - keep it once - and since shards own disjoint rows,
// concatenating their data rows needs no de-duplication or re-sorting.
std::string MergeSelectResults(const std::vector<std::string>& shard_results) {
    std::string header;
    std::vector<std::string> data_lines;
    for (const auto& result : shard_results) {
        std::istringstream iss(result);
        std::string line;
        bool first_line = true;
        while (std::getline(iss, line)) {
            if (first_line) {
                header = line;
                first_line = false;
                continue;
            }
            if (line == "(0 rows)") continue;
            data_lines.push_back(line);
        }
    }
    std::ostringstream out;
    out << header;
    for (const auto& line : data_lines) out << '\n' << line;
    if (data_lines.empty()) out << "\n(0 rows)";
    return out.str();
}

// PREPARE only locks keys - it doesn't otherwise know whether a staged
// command will actually succeed once COMMIT runs it. The one failure
// mode that's actually foreseeable ahead of time, given this project's
// simple schema (no other constraints), is an INSERT whose key already
// has a row: unlike UPDATE/DELETE (a no-op, not an error, if the row
// doesn't exist), that's a hard error every time. Catching it here,
// before any command in the batch has run, is what makes a PREPARE
// atomic with respect to its own multi-statement batch - a "no" vote
// here means literally nothing in the batch has run, rather than
// commands 1..k succeeding before command k+1 fails at COMMIT with no
// way to undo 1..k.
std::string FindStagedDuplicateKey(distdb::SqlExecutor& sql, distdb::StorageEngine& engine,
                                    const std::vector<std::string>& commands) {
    for (const auto& cmd : commands) {
        distdb::Statement stmt;
        try {
            distdb::Parser parser(distdb::Tokenize(cmd));
            stmt = parser.ParseStatement();
        } catch (const std::exception&) {
            continue;  // COMMIT will hit (and correctly report) this same parse error itself
        }
        if (!std::holds_alternative<distdb::InsertStatement>(stmt)) continue;
        auto key = sql.TryExtractRowKey(stmt);
        if (key && engine.Get(*key).has_value()) {
            return "duplicate primary key in staged command '" + cmd + "'";
        }
    }
    return "";
}

// One entry per buffered write statement between BEGIN and COMMIT: which
// shard its row key belongs to, that key (for the PREPARE's lock), and
// the raw SQL line (staged, to actually run once the transaction
// commits).
struct BufferedTxnStatement {
    distdb::ShardId shard_id;
    std::string key;
    std::string sql;
};

// Runs two-phase commit across every shard `buffer`'s statements touch:
// PREPARE each participant (locks its keys, stages its commands -
// nothing runs yet), and only if every one of them votes yes, tell them
// all to COMMIT (run the staged commands for real); if any votes no (or
// isn't reachable), tell whichever ones *did* prepare to ABORT instead
// (discard, release locks, nothing they hold ever became visible).
// Returns a human-readable outcome line for the REPL to print.
std::string RunTransaction(distdb::RoutingTable& routing, distdb::RaftNode& node, distdb::ShardId my_shard_id,
                            distdb::TxnId txn_id, const std::vector<BufferedTxnStatement>& buffer, int timeout_ms) {
    std::map<distdb::ShardId, distdb::TxnPrepare> by_shard;
    for (const auto& stmt : buffer) {
        auto& prepare = by_shard[stmt.shard_id];
        prepare.txn_id = txn_id;
        prepare.keys.push_back(stmt.key);
        prepare.commands.push_back(stmt.sql);
    }

    auto send_to_shard = [&](distdb::ShardId sid, const std::string& command) {
        if (sid == my_shard_id) return node.ProposeOrForward(command, timeout_ms);
        return distdb::SendClientRequest(routing.Shard(sid).peers, command, timeout_ms);
    };

    std::vector<distdb::ShardId> prepared_shards;
    std::string abort_reason;
    for (const auto& [sid, prepare] : by_shard) {
        distdb::ClientResponse resp;
        try {
            resp = send_to_shard(sid, distdb::EncodeTxnPrepare(prepare));
        } catch (const std::exception& e) {
            abort_reason = "shard " + std::to_string(sid) + ": " + e.what();
            break;
        }
        if (!resp.success) {
            abort_reason = "shard " + std::to_string(sid) + ": " + resp.error;
            break;
        }
        prepared_shards.push_back(sid);
    }

    if (!abort_reason.empty()) {
        std::vector<std::string> abort_failures;
        for (auto sid : prepared_shards) {
            try {
                auto resp = send_to_shard(sid, distdb::EncodeTxnAbort(txn_id));
                if (!resp.success) abort_failures.push_back("shard " + std::to_string(sid) + ": " + resp.error);
            } catch (const std::exception& e) {
                abort_failures.push_back("shard " + std::to_string(sid) + ": " + e.what());
            }
        }
        std::string msg = "ABORTED (" + abort_reason + ")";
        for (const auto& f : abort_failures) msg += " [rollback also failed on " + f + "]";
        return msg;
    }

    std::vector<std::string> commit_failures;
    for (const auto& [sid, prepare] : by_shard) {
        (void)prepare;
        try {
            auto resp = send_to_shard(sid, distdb::EncodeTxnCommit(txn_id));
            if (!resp.success) commit_failures.push_back("shard " + std::to_string(sid) + ": " + resp.error);
        } catch (const std::exception& e) {
            commit_failures.push_back("shard " + std::to_string(sid) + ": " + e.what());
        }
    }

    if (commit_failures.empty()) {
        return "OK (committed transaction " + std::to_string(txn_id) + " across " +
               std::to_string(by_shard.size()) + " shard(s))";
    }
    std::string msg = "transaction " + std::to_string(txn_id) +
                       " was DECIDED TO COMMIT but couldn't notify every shard:";
    for (const auto& f : commit_failures) msg += " [" + f + "]";
    msg += " - those shard(s) may still be holding locks; retrying the same COMMIT is safe once they're reachable";
    return msg;
}

}  // namespace

int main(int argc, char** argv) {
    // stdout is often redirected to a log file when running several
    // nodes as background processes; without unitbuf, output sits in a
    // buffer indefinitely since this process never exits on its own.
    std::cout.setf(std::ios::unitbuf);

    if (argc < 5) {
        std::cerr << "usage: raftnode <node-id> <shard-id> <listen-port> <routing-table-path> [state-dir] "
                      "[--joining <own-host>]\n";
        return 1;
    }

    try {
        auto node_id = static_cast<distdb::NodeId>(std::stoul(argv[1]));
        auto shard_id = static_cast<distdb::ShardId>(std::stoul(argv[2]));
        auto port = static_cast<uint16_t>(std::stoul(argv[3]));
        std::string routing_path = argv[4];
        std::string state_dir = argc > 5 ? argv[5] : ".";
        std::filesystem::create_directories(state_dir);

        // A node bootstrapped via --joining isn't listed in anyone's
        // routing.conf row yet (it's being added at runtime through an
        // add-server command) - it starts with no known peers and must
        // be told its own reachable address explicitly, since there's no
        // other source of truth for it until membership_state_ learns
        // better from a real leader's AppendEntries/InstallSnapshot.
        bool joining = false;
        std::string own_host;
        if (argc > 6 && std::string(argv[6]) == "--joining") {
            if (argc <= 7) {
                std::cerr << "usage: --joining requires an <own-host> argument\n";
                return 1;
            }
            joining = true;
            own_host = argv[7];
        }

        distdb::RoutingTable routing;
        routing.LoadFromFile(routing_path);

        // A node added via add-server is never listed in routing.conf (it's
        // a static, hand-edited file this feature deliberately doesn't
        // touch), so the "must be in routing.conf" check below would wrongly
        // reject it on every restart *after* the first, not just refuse a
        // genuine bootstrap. Its own membership file - written the first
        // time it successfully joined - is what settles that on every later
        // run, checked here before RaftNode (and hence before
        // MembershipState::Load()) even exists, so --joining only ever
        // needs to be passed once.
        bool has_membership_file =
            std::filesystem::exists(distdb::MembershipState::PathFor(state_dir, node_id));

        const auto& my_shard = routing.Shard(shard_id);
        distdb::PeerAddress own_address;
        std::map<distdb::NodeId, distdb::PeerAddress> peers_in_shard;
        if (has_membership_file) {
            // Already run (and been added) before - RaftNode's constructor
            // will load the real membership from its own file and ignore
            // these; routing.conf doesn't need to (and generally won't)
            // list this node at all.
            own_address = {"", port};
        } else if (joining) {
            own_address = {own_host, port};
        } else {
            if (my_shard.peers.find(node_id) == my_shard.peers.end()) {
                throw std::runtime_error("node " + std::to_string(node_id) + " is not listed as a peer of shard " +
                                          std::to_string(shard_id) + " in " + routing_path);
            }
            own_address = my_shard.peers.at(node_id);
            peers_in_shard = my_shard.peers;
            peers_in_shard.erase(node_id);  // RaftNode's peers_ convention: exclude self
        }

        distdb::StorageEngine engine(state_dir + "/kv");
        engine.Open();
        distdb::SqlExecutor sql(engine);

        // StorageEngine/SqlExecutor have no locking of their own; this
        // one mutex guards every access, from the REPL thread, the apply
        // path, the snapshot path, and now local reads served for a
        // remote shard's SendReadRequest.
        std::mutex engine_mutex;

        // This shard's row-key locks and staged commands for every
        // transaction currently prepared here - see txn/participant.h.
        // Guarded by engine_mutex too: every call into it happens from
        // inside apply_callback, which already holds it for the SQL side
        // of the same apply.
        distdb::TxnParticipant txn_participant;

        auto apply_callback = [&sql, &engine, &engine_mutex, &txn_participant, node_id](
                                   distdb::LogIndex index, const std::string& command) -> std::string {
            std::lock_guard<std::mutex> lock(engine_mutex);

            if (auto control = distdb::TryDecodeTxnControl(command)) {
                switch (control->type) {
                    case distdb::TxnControlType::kPrepare: {
                        std::cout << "[node " << node_id << "] applying #" << index << ": PREPARE txn "
                                  << control->txn_id << " (" << control->keys.size() << " key(s))\n";
                        std::string error = FindStagedDuplicateKey(sql, engine, control->commands);
                        if (error.empty()) {
                            error = txn_participant.Prepare(control->txn_id, control->keys, control->commands);
                        }
                        if (!error.empty()) std::cout << "[node " << node_id << "] prepare vote NO: " << error << "\n";
                        return error;
                    }
                    case distdb::TxnControlType::kCommit: {
                        std::cout << "[node " << node_id << "] applying #" << index << ": COMMIT txn "
                                  << control->txn_id << "\n";
                        try {
                            return txn_participant.Commit(control->txn_id,
                                                           [&](const std::string& staged) { sql.Execute(staged); });
                        } catch (const std::exception& e) {
                            std::cout << "[node " << node_id << "] apply error inside committed txn "
                                      << control->txn_id << ": " << e.what() << "\n";
                            return std::string(e.what());
                        }
                    }
                    case distdb::TxnControlType::kAbort:
                        std::cout << "[node " << node_id << "] applying #" << index << ": ABORT txn "
                                  << control->txn_id << "\n";
                        return txn_participant.Abort(control->txn_id);
                }
            }

            std::cout << "[node " << node_id << "] applying #" << index << ": " << command << "\n";
            try {
                sql.Execute(command);
                return "";
            } catch (const std::exception& e) {
                std::cout << "[node " << node_id << "] apply error: " << e.what() << "\n";
                return e.what();
            }
        };

        auto snapshot_callback = [&engine, &engine_mutex, node_id]() -> std::string {
            std::lock_guard<std::mutex> lock(engine_mutex);
            auto kvs = engine.Scan("");
            std::cout << "[node " << node_id << "] building snapshot: " << kvs.size() << " key(s)\n";
            return EncodeSnapshot(kvs);
        };

        auto restore_callback = [&engine, &engine_mutex, node_id](const std::string& data) {
            std::lock_guard<std::mutex> lock(engine_mutex);
            auto kvs = DecodeSnapshot(data);
            std::cout << "[node " << node_id << "] installing snapshot: " << kvs.size() << " key(s)\n";
            try {
                engine.Reset();
                for (const auto& [key, value] : kvs) {
                    engine.Put(key, value);
                }
            } catch (const std::exception& e) {
                std::cout << "[node " << node_id << "] snapshot install FAILED: " << e.what() << "\n";
            }
        };

        auto read_callback = [&sql, &engine_mutex](const std::string& query) -> std::string {
            std::lock_guard<std::mutex> lock(engine_mutex);
            return sql.Execute(query);
        };

        distdb::RaftNode node(node_id, port, own_address, peers_in_shard, state_dir, joining, apply_callback,
                              snapshot_callback, restore_callback, read_callback);

        std::cout << "raftnode " << node_id << " (shard " << shard_id << ") listening on port " << port << "\n";
        node.Run();
        PrintHelp();

        // Transaction-buffering state: while `in_txn`, writes are queued
        // here instead of running immediately - see the "begin" handling
        // below. txn_counter combines with this node's own id to make
        // every txn_id it ever generates globally unique without needing
        // a dedicated sequencer (see RunTransaction's caller).
        bool in_txn = false;
        std::vector<BufferedTxnStatement> txn_buffer;
        uint32_t txn_counter = 0;

        std::string line;
        while (std::getline(std::cin, line)) {
            // Some redirected-stdin setups (e.g. a .NET StreamWriter
            // piping into this process, as used by this project's test
            // scripts) emit a UTF-8 BOM before the very first byte
            // written. Strip it defensively so it doesn't get parsed as
            // part of the first command.
            if (line.size() >= 3 && static_cast<unsigned char>(line[0]) == 0xEF &&
                static_cast<unsigned char>(line[1]) == 0xBB && static_cast<unsigned char>(line[2]) == 0xBF) {
                line.erase(0, 3);
            }

            std::istringstream iss(line);
            std::string first_word;
            iss >> first_word;
            if (first_word.empty()) continue;

            if (first_word == "exit" || first_word == "quit") {
                break;
            } else if (first_word == "help") {
                PrintHelp();
                continue;
            } else if (first_word == "status") {
                std::cout << "shard=" << shard_id << " role=" << RoleName(node.role())
                          << " term=" << node.current_term() << " leader_hint=" << node.leader_hint() << "\n";
                continue;
            } else if (first_word == "begin") {
                if (in_txn) {
                    std::cout << "ERROR: already in a transaction - commit or rollback it first\n";
                } else {
                    in_txn = true;
                    txn_buffer.clear();
                    std::cout << "OK (transaction started)\n";
                }
                continue;
            } else if (first_word == "rollback") {
                if (!in_txn) {
                    std::cout << "ERROR: not in a transaction\n";
                } else {
                    in_txn = false;
                    txn_buffer.clear();
                    std::cout << "OK (rolled back - nothing was ever sent)\n";
                }
                continue;
            } else if (first_word == "commit") {
                if (!in_txn) {
                    std::cout << "ERROR: not in a transaction\n";
                } else if (txn_buffer.empty()) {
                    in_txn = false;
                    std::cout << "OK (empty transaction, nothing to commit)\n";
                } else {
                    in_txn = false;
                    auto txn_id = (static_cast<distdb::TxnId>(node_id) << 32) | (++txn_counter);
                    std::cout << RunTransaction(routing, node, shard_id, txn_id, txn_buffer, kCrossShardTimeoutMs)
                              << "\n";
                    txn_buffer.clear();
                }
                continue;
            } else if (first_word == "add-server") {
                unsigned long new_id_raw = 0, new_port_raw = 0;
                std::string host;
                if (!(iss >> new_id_raw >> host >> new_port_raw)) {
                    std::cout << "ERROR: usage: add-server <id> <host> <port>\n";
                } else {
                    distdb::ConfChangeCommand cc{distdb::ConfChangeType::kAddServer,
                                                  static_cast<distdb::NodeId>(new_id_raw), host,
                                                  static_cast<uint16_t>(new_port_raw)};
                    PrintClientResponse(node.ProposeOrForward(distdb::EncodeConfChange(cc)));
                }
                continue;
            } else if (first_word == "remove-server") {
                unsigned long target_id_raw = 0;
                if (!(iss >> target_id_raw)) {
                    std::cout << "ERROR: usage: remove-server <id>\n";
                } else {
                    distdb::ConfChangeCommand cc{distdb::ConfChangeType::kRemoveServer,
                                                  static_cast<distdb::NodeId>(target_id_raw), "", 0};
                    PrintClientResponse(node.ProposeOrForward(distdb::EncodeConfChange(cc)));
                }
                continue;
            }

            distdb::Statement parsed;
            try {
                distdb::Parser parser(distdb::Tokenize(line));
                parsed = parser.ParseStatement();
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
                continue;
            }
            StatementTimer timer;  // prints elapsed time once this statement's dispatch below finishes

            if (in_txn) {
                // Deliberately narrow: only a statement that pins one
                // row's primary key can be staged as one participant
                // shard's share of a PREPARE. CREATE TABLE/ALTER TABLE are
                // schema, not a row, and a whole-table SELECT/UPDATE/DELETE
                // has no single shard to lock it against - see the
                // scatter/gather path above for those, which already has no
                // cross-shard atomicity story of its own to begin with.
                if (std::holds_alternative<distdb::CreateTableStatement>(parsed) ||
                    std::holds_alternative<distdb::AlterTableAddColumnStatement>(parsed)) {
                    std::cout << "ERROR: CREATE TABLE/ALTER TABLE are not supported inside a transaction\n";
                    continue;
                }
                if (std::holds_alternative<distdb::SelectStatement>(parsed) ||
                    std::holds_alternative<distdb::ShowTablesStatement>(parsed)) {
                    std::cout << "ERROR: SELECT/SHOW TABLES are not supported inside a transaction (writes are "
                                 "staged, not applied, until commit - a read here could never see them anyway); "
                                 "run it before begin or after commit/rollback\n";
                    continue;
                }
                auto key = sql.TryExtractRowKey(parsed);
                if (!key) {
                    std::cout << "ERROR: this statement's WHERE doesn't pin the primary key, so it has no single "
                                 "shard to lock - not supported inside a transaction\n";
                    continue;
                }
                try {
                    distdb::ShardId sid = routing.ShardFor(*key).id;
                    txn_buffer.push_back({sid, *key, line});
                    std::cout << "OK (queued on shard " << sid << ")\n";
                } catch (const std::exception& e) {
                    std::cout << "ERROR: " << e.what() << "\n";
                }
                continue;
            }

            if (std::holds_alternative<distdb::CreateTableStatement>(parsed)) {
                // Schema is DDL, not row data - any shard could end up
                // holding rows for this table, so every shard needs an
                // identical copy of it, not just the one (if any) that
                // would own some specific key.
                PrintBroadcastResult(BroadcastWrite(routing, node, shard_id, line, kCrossShardTimeoutMs), "created");
                continue;
            }
            if (std::holds_alternative<distdb::AlterTableAddColumnStatement>(parsed)) {
                // Same reasoning as CREATE TABLE above - every shard's copy
                // of this table's schema needs the new column, since a row
                // for this table could exist on any of them.
                PrintBroadcastResult(BroadcastWrite(routing, node, shard_id, line, kCrossShardTimeoutMs), "altered");
                continue;
            }
            if (std::holds_alternative<distdb::ShowTablesStatement>(parsed)) {
                // The opposite of CREATE/ALTER TABLE above: a read, and
                // every shard already has an identical copy of every
                // table's schema (that's exactly what broadcasting CREATE/
                // ALTER TABLE guarantees), so this node's own shard alone
                // already has the complete, correct answer - no need to
                // scatter to every shard and merge like a row-data SELECT
                // with no WHERE would need.
                try {
                    std::lock_guard<std::mutex> lock(engine_mutex);
                    std::cout << sql.Execute(line) << "\n";
                } catch (const std::exception& e) {
                    std::cout << "ERROR: " << e.what() << "\n";
                }
                continue;
            }

            bool is_select = std::holds_alternative<distdb::SelectStatement>(parsed);
            auto key = sql.TryExtractRowKey(parsed);

            if (!key) {
                // No WHERE <pk> = ... to pin this to one shard: scatter
                // to every shard and gather the results/counts back into
                // one answer, rather than only ever seeing this node's
                // own shard's slice of the table.
                if (is_select) {
                    std::vector<std::string> shard_results;
                    std::vector<std::string> failures;
                    for (auto sid : routing.AllShardIds()) {
                        try {
                            if (sid == shard_id) {
                                std::lock_guard<std::mutex> lock(engine_mutex);
                                shard_results.push_back(sql.Execute(line));
                            } else {
                                shard_results.push_back(
                                    distdb::SendReadRequest(routing.Shard(sid).peers, line, kCrossShardTimeoutMs));
                            }
                        } catch (const std::exception& e) {
                            failures.push_back("shard " + std::to_string(sid) + ": " + e.what());
                        }
                    }
                    if (!failures.empty()) {
                        std::cout << "ERROR: some shards did not respond:";
                        for (const auto& f : failures) std::cout << " [" << f << "]";
                        std::cout << "\n";
                    } else {
                        std::cout << MergeSelectResults(shard_results) << "\n";
                    }
                } else {
                    // UPDATE/DELETE without a primary-key filter: each
                    // shard independently applies the same WHERE
                    // predicate to whatever rows it holds - correct with
                    // no cross-shard coordination, since shards own
                    // disjoint rows (this is not an atomic multi-shard
                    // transaction, just N independent single-shard ones).
                    PrintBroadcastResult(BroadcastWrite(routing, node, shard_id, line, kCrossShardTimeoutMs),
                                         "applied");
                }
                continue;
            }

            const distdb::ShardRange* target = nullptr;
            try {
                target = &routing.ShardFor(*key);
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
                continue;
            }

            bool local = target->id == shard_id;

            if (is_select) {
                try {
                    std::string result;
                    if (local) {
                        std::lock_guard<std::mutex> lock(engine_mutex);
                        result = sql.Execute(line);
                    } else {
                        result = distdb::SendReadRequest(target->peers, line, kCrossShardTimeoutMs);
                    }
                    std::cout << result << "\n";
                } catch (const std::exception& e) {
                    std::cout << "ERROR: " << e.what() << "\n";
                }
            } else if (local) {
                PrintClientResponse(node.ProposeOrForward(line));
            } else {
                try {
                    PrintClientResponse(distdb::SendClientRequest(target->peers, line, kCrossShardTimeoutMs));
                } catch (const std::exception& e) {
                    std::cout << "ERROR: " << e.what() << "\n";
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
