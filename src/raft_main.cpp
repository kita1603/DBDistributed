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

#include "raft/node.h"
#include "sql/ast.h"
#include "sql/executor.h"
#include "sql/lexer.h"
#include "sql/parser.h"
#include "storage/engine.h"

namespace {

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

// Parses "2=127.0.0.1:9002,3=127.0.0.1:9003" into a peer map (this
// node's own id is not included).
std::map<distdb::NodeId, distdb::PeerAddress> ParsePeers(const std::string& spec) {
    std::map<distdb::NodeId, distdb::PeerAddress> peers;
    std::istringstream stream(spec);
    std::string entry;
    while (std::getline(stream, entry, ',')) {
        if (entry.empty()) continue;
        size_t eq = entry.find('=');
        size_t colon = entry.find(':', eq == std::string::npos ? 0 : eq);
        if (eq == std::string::npos || colon == std::string::npos) {
            throw std::runtime_error("bad peer spec: " + entry);
        }
        auto id = static_cast<distdb::NodeId>(std::stoul(entry.substr(0, eq)));
        std::string host = entry.substr(eq + 1, colon - eq - 1);
        auto port = static_cast<uint16_t>(std::stoul(entry.substr(colon + 1)));
        peers[id] = {host, port};
    }
    return peers;
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

void PrintHelp() {
    std::cout << "Enter SQL statements (CREATE TABLE / INSERT / SELECT / UPDATE / DELETE).\n"
                 "Writes (CREATE/INSERT/UPDATE/DELETE) are replicated through Raft - proposed\n"
                 "to the log and only committed once a majority of nodes hold them, so they\n"
                 "only succeed when sent to the current leader. SELECT reads straight from\n"
                 "this node's local storage - not linearizable, so a follower can be a little\n"
                 "behind the leader if you read right after a write.\n"
                 "\n"
                 "  status   show role / term / current leader hint\n"
                 "  help\n"
                 "  exit\n";
}

}  // namespace

int main(int argc, char** argv) {
    // stdout is often redirected to a log file when running several
    // nodes as background processes; without unitbuf, output sits in a
    // buffer indefinitely since this process never exits on its own.
    std::cout.setf(std::ios::unitbuf);

    if (argc < 4) {
        std::cerr << "usage: raftnode <id> <listen-port> <peers> [state-dir]\n"
                     "  peers: \"2=127.0.0.1:9002,3=127.0.0.1:9003\" (this node's own id excluded)\n";
        return 1;
    }

    try {
        auto id = static_cast<distdb::NodeId>(std::stoul(argv[1]));
        auto port = static_cast<uint16_t>(std::stoul(argv[2]));
        auto peers = ParsePeers(argv[3]);
        std::string state_dir = argc > 4 ? argv[4] : ".";
        std::filesystem::create_directories(state_dir);

        distdb::StorageEngine engine(state_dir + "/kv");
        engine.Open();
        distdb::SqlExecutor sql(engine);

        // StorageEngine/SqlExecutor have no locking of their own (Phase
        // 1 only ever used them from a single thread). Now that they're
        // reached from several: the REPL's own thread (SELECT), whatever
        // thread is applying a just-committed entry, and whatever thread
        // is building or installing a snapshot - all access goes through
        // this one mutex.
        std::mutex engine_mutex;

        // Every node applies the exact same sequence of committed SQL
        // statements to its own StorageEngine, which is what makes them
        // end up with identical data. A statement that fails here (e.g.
        // a duplicate primary key) fails the same way on every node,
        // since they've all applied the identical sequence of prior
        // statements - so this never causes replicas to diverge. Must
        // not let the exception escape: see the ApplyCallback contract
        // in raft/node.h.
        auto apply_callback = [&sql, &engine_mutex, id](distdb::LogIndex index, const std::string& command) {
            std::cout << "[node " << id << "] applying #" << index << ": " << command << "\n";
            std::lock_guard<std::mutex> lock(engine_mutex);
            try {
                sql.Execute(command);
            } catch (const std::exception& e) {
                std::cout << "[node " << id << "] apply error: " << e.what() << "\n";
            }
        };

        // Called when a badly-lagging follower needs this node's (the
        // leader's) entire current state, because the log entries it
        // would otherwise need have already been compacted away.
        auto snapshot_callback = [&engine, &engine_mutex, id]() -> std::string {
            std::lock_guard<std::mutex> lock(engine_mutex);
            auto kvs = engine.Scan("");
            std::cout << "[node " << id << "] building snapshot: " << kvs.size() << " key(s)\n";
            return EncodeSnapshot(kvs);
        };

        // Called on the receiving end of the above: replace this node's
        // entire state with what's in the snapshot, discarding whatever
        // was here before - a wholesale replacement, not a merge.
        auto restore_callback = [&engine, &engine_mutex, id](const std::string& data) {
            std::lock_guard<std::mutex> lock(engine_mutex);
            auto kvs = DecodeSnapshot(data);
            std::cout << "[node " << id << "] installing snapshot: " << kvs.size() << " key(s)\n";
            try {
                engine.Reset();
                for (const auto& [key, value] : kvs) {
                    engine.Put(key, value);
                }
            } catch (const std::exception& e) {
                std::cout << "[node " << id << "] snapshot install FAILED: " << e.what() << "\n";
            }
        };

        distdb::RaftNode node(id, port, peers, state_dir, apply_callback, snapshot_callback, restore_callback);

        std::cout << "raftnode " << id << " listening on port " << port << ", peers: " << argv[3] << "\n";
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
                std::cout << "role=" << RoleName(node.role()) << " term=" << node.current_term()
                          << " leader_hint=" << node.leader_hint() << "\n";
                continue;
            }

            // Anything else is a SQL statement. Parse it first just to
            // classify read vs. write - a syntax error is caught here
            // without ever being proposed to the log.
            distdb::Statement parsed;
            try {
                distdb::Parser parser(distdb::Tokenize(line));
                parsed = parser.ParseStatement();
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
                continue;
            }

            if (std::holds_alternative<distdb::SelectStatement>(parsed)) {
                // Read-only: served straight from local storage, no
                // consensus round needed. Still needs engine_mutex - a
                // commit could be getting applied by another thread
                // concurrently.
                try {
                    std::lock_guard<std::mutex> lock(engine_mutex);
                    std::cout << sql.Execute(line) << "\n";
                } catch (const std::exception& e) {
                    std::cout << "ERROR: " << e.what() << "\n";
                }
            } else {
                // ProposeOrForward means this REPL doesn't need to be
                // pointed at the leader itself: if this node isn't it,
                // the request gets forwarded over the network to
                // whichever node is (or several, if that guess is
                // stale) before the result comes back.
                auto resp = node.ProposeOrForward(line);
                if (resp.success) {
                    std::cout << "OK (committed at index " << resp.index << ", via leader node "
                               << resp.leader_hint << ")\n";
                } else {
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
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
