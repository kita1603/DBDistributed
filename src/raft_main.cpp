#include <cstdint>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

#include "raft/client.h"
#include "raft/node.h"
#include "shard/routing.h"
#include "sql/ast.h"
#include "sql/executor.h"
#include "sql/lexer.h"
#include "sql/parser.h"
#include "storage/engine.h"

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
    std::cout << "Enter SQL statements (CREATE TABLE / INSERT / SELECT / UPDATE / DELETE).\n"
                 "CREATE TABLE is broadcast to every shard. INSERT, and any SELECT/UPDATE/\n"
                 "DELETE whose WHERE pins the primary key with '=', is routed to the one\n"
                 "shard that owns it (replicated through Raft the same as a single-shard\n"
                 "cluster, forwarding over the network first if that shard isn't this node's).\n"
                 "A statement that can't be pinned to one shard (no WHERE <pk> = ...) is\n"
                 "rejected for now - cross-shard table scans aren't implemented yet.\n"
                 "\n"
                 "  status   show role / term / current leader hint / shard id\n"
                 "  help\n"
                 "  exit\n";
}

}  // namespace

int main(int argc, char** argv) {
    // stdout is often redirected to a log file when running several
    // nodes as background processes; without unitbuf, output sits in a
    // buffer indefinitely since this process never exits on its own.
    std::cout.setf(std::ios::unitbuf);

    if (argc < 5) {
        std::cerr << "usage: raftnode <node-id> <shard-id> <listen-port> <routing-table-path> [state-dir]\n";
        return 1;
    }

    try {
        auto node_id = static_cast<distdb::NodeId>(std::stoul(argv[1]));
        auto shard_id = static_cast<distdb::ShardId>(std::stoul(argv[2]));
        auto port = static_cast<uint16_t>(std::stoul(argv[3]));
        std::string routing_path = argv[4];
        std::string state_dir = argc > 5 ? argv[5] : ".";
        std::filesystem::create_directories(state_dir);

        distdb::RoutingTable routing;
        routing.LoadFromFile(routing_path);

        const auto& my_shard = routing.Shard(shard_id);
        if (my_shard.peers.find(node_id) == my_shard.peers.end()) {
            throw std::runtime_error("node " + std::to_string(node_id) + " is not listed as a peer of shard " +
                                      std::to_string(shard_id) + " in " + routing_path);
        }
        std::map<distdb::NodeId, distdb::PeerAddress> peers_in_shard = my_shard.peers;
        peers_in_shard.erase(node_id);  // RaftNode's peers_ convention: exclude self

        distdb::StorageEngine engine(state_dir + "/kv");
        engine.Open();
        distdb::SqlExecutor sql(engine);

        // StorageEngine/SqlExecutor have no locking of their own; this
        // one mutex guards every access, from the REPL thread, the apply
        // path, the snapshot path, and now local reads served for a
        // remote shard's SendReadRequest.
        std::mutex engine_mutex;

        auto apply_callback = [&sql, &engine_mutex, node_id](distdb::LogIndex index, const std::string& command) {
            std::cout << "[node " << node_id << "] applying #" << index << ": " << command << "\n";
            std::lock_guard<std::mutex> lock(engine_mutex);
            try {
                sql.Execute(command);
            } catch (const std::exception& e) {
                std::cout << "[node " << node_id << "] apply error: " << e.what() << "\n";
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

        distdb::RaftNode node(node_id, port, peers_in_shard, state_dir, apply_callback, snapshot_callback,
                               restore_callback, read_callback);

        std::cout << "raftnode " << node_id << " (shard " << shard_id << ") listening on port " << port << "\n";
        node.Run();
        PrintHelp();

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
            }

            distdb::Statement parsed;
            try {
                distdb::Parser parser(distdb::Tokenize(line));
                parsed = parser.ParseStatement();
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
                continue;
            }

            if (std::holds_alternative<distdb::CreateTableStatement>(parsed)) {
                // Schema is DDL, not row data - any shard could end up
                // holding rows for this table, so every shard needs an
                // identical copy of it, not just the one (if any) that
                // would own some specific key.
                size_t ok_count = 0;
                std::vector<std::string> failures;
                for (auto sid : routing.AllShardIds()) {
                    distdb::ClientResponse resp;
                    if (sid == shard_id) {
                        resp = node.ProposeOrForward(line);
                    } else {
                        try {
                            resp = distdb::SendClientRequest(routing.Shard(sid).peers, line, kCrossShardTimeoutMs);
                        } catch (const std::exception& e) {
                            resp = {false, 0, true, 0, e.what()};
                        }
                    }
                    if (resp.success) {
                        ok_count++;
                    } else {
                        failures.push_back("shard " + std::to_string(sid) + ": " + resp.error);
                    }
                }
                if (failures.empty()) {
                    std::cout << "OK (created on " << ok_count << " shard(s))\n";
                } else {
                    std::cout << "PARTIAL (" << ok_count << " shard(s) ok):";
                    for (const auto& f : failures) std::cout << " [" << f << "]";
                    std::cout << "\n";
                }
                continue;
            }

            auto key = sql.TryExtractRowKey(parsed);
            if (!key) {
                std::cout << "ERROR: cannot route this statement to a single shard (no WHERE <primary-key> = ... "
                             "condition) - cross-shard table scans aren't supported yet\n";
                continue;
            }

            const distdb::ShardRange* target = nullptr;
            try {
                target = &routing.ShardFor(*key);
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
                continue;
            }

            bool is_select = std::holds_alternative<distdb::SelectStatement>(parsed);
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
