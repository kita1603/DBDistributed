#include <algorithm>
#include <cctype>
#include <iostream>
#include <sstream>
#include <string>

#include "sql/executor.h"
#include "storage/engine.h"

namespace {

void PrintHelp() {
    std::cout << "Raw KV commands:\n"
                 "  put <key> <value>\n"
                 "  get <key>\n"
                 "  del <key>\n"
                 "\n"
                 "SQL statements (single line each):\n"
                 "  CREATE TABLE t (id TEXT PRIMARY KEY, name TEXT, age INT)\n"
                 "  INSERT INTO t (id, name, age) VALUES ('u1', 'Alice', 30)\n"
                 "  SELECT * FROM t WHERE age > 20\n"
                 "  UPDATE t SET age = 31 WHERE id = 'u1'\n"
                 "  DELETE FROM t WHERE id = 'u1'\n"
                 "\n"
                 "  help\n"
                 "  exit\n";
}

std::string ToUpper(const std::string& s) {
    std::string out = s;
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) { return std::toupper(c); });
    return out;
}

}  // namespace

int main(int argc, char** argv) {
    std::string db_path = argc > 1 ? argv[1] : "./data";

    distdb::StorageEngine engine(db_path);
    engine.Open();
    distdb::SqlExecutor sql(engine);

    std::cout << "distdb (Phase 1: storage engine + SQL). Data dir: " << db_path << "\n";
    PrintHelp();

    std::string line;
    while (true) {
        std::cout << "> ";
        if (!std::getline(std::cin, line)) break;

        std::istringstream iss(line);
        std::string cmd;
        iss >> cmd;

        if (cmd.empty()) {
            continue;
        }

        std::string upper_cmd = ToUpper(cmd);
        if (upper_cmd == "EXIT" || upper_cmd == "QUIT") {
            break;
        } else if (upper_cmd == "HELP") {
            PrintHelp();
        } else if (upper_cmd == "CREATE" || upper_cmd == "INSERT" || upper_cmd == "SELECT" ||
                   upper_cmd == "UPDATE" || upper_cmd == "DELETE") {
            try {
                std::cout << sql.Execute(line) << "\n";
            } catch (const std::exception& e) {
                std::cout << "ERROR: " << e.what() << "\n";
            }
        } else if (cmd == "put") {
            std::string key;
            iss >> key;
            std::string value;
            std::getline(iss, value);
            if (!value.empty() && value.front() == ' ') value.erase(0, 1);
            if (key.empty() || value.empty()) {
                std::cout << "usage: put <key> <value>\n";
                continue;
            }
            engine.Put(key, value);
            std::cout << "OK\n";
        } else if (cmd == "get") {
            std::string key;
            iss >> key;
            if (key.empty()) {
                std::cout << "usage: get <key>\n";
                continue;
            }
            auto value = engine.Get(key);
            std::cout << (value ? *value : "(not found)") << "\n";
        } else if (cmd == "del") {
            std::string key;
            iss >> key;
            if (key.empty()) {
                std::cout << "usage: del <key>\n";
                continue;
            }
            engine.Delete(key);
            std::cout << "OK\n";
        } else {
            std::cout << "unknown command: " << cmd << " (type 'help')\n";
        }
    }

    return 0;
}
