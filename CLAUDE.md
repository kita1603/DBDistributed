# distdb

Distributed SQL database built from scratch in C++, for learning purposes -
no external DB/consensus libraries (no RocksDB, braft, NuRaft, etc.). See
README.md for the full design writeup per phase (storage engine, Raft,
sharding, transactions) - it's kept current and is the source of truth for
"why" something is built the way it is. This file is the quick-reference for
"how to work in this repo."

## Status

- **Phase 1** (`src/storage/`, `src/sql/`): single-node LSM storage engine
  (WAL + memtable + SSTable + compaction) and a SQL layer (lexer/parser/
  executor) on top. Done.
- **Phase 2** (`src/raft/`): Raft consensus from scratch - election,
  heartbeats, log replication, leader-redirect, log compaction +
  InstallSnapshot. Done.
- **Phase 3** (`src/shard/`): static range-based sharding - routing table,
  cross-shard forwarding, scatter/gather for whole-table statements. Done.
- **Phase 4** (`src/txn/`): distributed transactions via two-phase commit,
  layered on Raft/sharding with no storage or RPC changes. Done.
- **Phase 5** (`src/raft/conf_change.h`, `membership_state.h`): single-
  server-at-a-time cluster membership changes (`add-server`/`remove-server`
  REPL commands), scoped to one shard's Raft group at a time. Done. Dynamic
  *resharding* (splitting/merging a shard's key range, rebalancing) is a
  separate, not-yet-started follow-up - `config/routing.conf` is still a
  hand-edited static file, unaffected by membership changes.

The user is a beginner to distributed systems and C++ working through this
as a guided build - explain *why*, not just *what*, and check in on design
choices (2PC vs. Percolator, hash vs. range sharding, etc.) rather than just
picking one silently.

## Build & run

Windows, MinGW (GCC 16 via `BrechtSanders.WinLibs.POSIX.UCRT`), Ninja
generator (mingw32-make has a readdir bug that breaks this project's build).

```
cmake -S . -B build -G Ninja
cmake --build build
```

`raftnode.exe` is the real executable (`distdb.exe` is a Phase-1-only
leftover REPL with no Raft, rarely relevant). Usage:

```
raftnode.exe <node-id> <shard-id> <port> <routing-table-path> [state-dir] [--joining <own-host>]
```

`--joining <own-host>` is only for a node's first-ever run when it's being
added to an already-running shard via the `add-server` REPL command rather
than listed in `routing.conf` from the start - see README's Phase 5 section.
Every later restart of that same node omits it; its own persisted
membership file is authoritative by then.

**Before rebuilding, make sure no `raftnode.exe` instances are running** -
Windows locks the file while it's running and the link step fails with
"Permission denied". Check with `tasklist | grep -i raftnode` (Bash tool) and
ask the user to close their terminal windows if any are running - don't kill
a user's process without asking, they may be mid-test.

## Architecture map

- `src/storage/` - WAL, memtable, SSTable, `StorageEngine` (opaque KV, no SQL
  knowledge).
- `src/sql/` - lexer/parser/AST/executor. Maps SQL tables/rows onto the KV
  space: `__schema__/<table>`, `__row__/<table>/<pk>`. `TryExtractRowKey`
  (executor.h) is the key integration point sharding/txn both depend on: it
  tells the caller which single row (if any) a statement deterministically
  touches.
- `src/raft/` - `RaftNode` (the consensus state machine), `RaftLog`,
  `RaftTransport` (one-shot TCP connection per RPC), `client.h/.cpp`
  (`SendClientRequest`/`SendReadRequest` for reaching a shard the caller
  isn't a member of). Log entries hold opaque `command` bytes - Raft itself
  never interprets them. `conf_change.h/.cpp` encodes add/remove-server
  commands the same way `txn.h` encodes PREPARE/COMMIT/ABORT (leading
  `0x01` byte, txn already claims `0x00`); `membership_state.h/.cpp`
  persists a shard's membership the same way `RaftLog::applied_index_`
  persists how far apply has gotten - both exist so something a compacted-
  away log entry once said isn't forgotten across a restart.
- `src/shard/` - `RoutingTable`: static text file mapping `[start_key,
  end_key)` ranges to a shard id + peer list. Loaded once at startup, never
  changes at runtime - unaffected by Phase 5's membership changes; dynamic
  resharding (not yet started) is what would eventually replace this.
- `src/txn/` - `txn.h/.cpp` encodes PREPARE/COMMIT/ABORT as tagged opaque
  commands (leading `0x00` byte - no real SQL statement can start with
  that); `participant.h/.cpp` (`TxnParticipant`) holds one shard's lock
  table + staged-but-unrun commands for in-flight transactions.
- `src/raft_main.cpp` - the actual application: wires storage+SQL+Raft+
  sharding+txn together, and is the REPL (`begin`/`commit`/`rollback`/
  `status`/`help`/`exit`/`add-server`/`remove-server` plus SQL statements).
  Most cross-cutting logic (routing decisions, scatter/gather, 2PC
  coordination) lives here rather than in the libraries, since it's the one
  place that knows about all of them at once.
- `src/ui/main.cpp` (+ vendored `third_party/imgui/`) - `raftui`, a
  Windows-only Dear ImGui desktop client. Links `distdb_raft`/`distdb_sql`
  directly and calls the same `client.h` functions/SQL parser
  `raft_main.cpp` itself uses - a graphical single-shard REPL, not a
  front-end for cross-shard/txn features. See README's `raftui` section.

Everything is opaque-bytes-in-a-log turtles-all-the-way-down: Raft doesn't
know about SQL, SQL doesn't know about shards, shards don't know about
transactions - each layer just hands the one below it a byte string.

## Testing conventions

No automated test suite - everything is verified by hand, by actually
running one or more `raftnode.exe` processes and driving them through the
REPL. This project's own test harness *is* a PowerShell script per test,
using `System.Diagnostics.Process` with redirected stdin/stdout (see recent
session transcripts for the exact pattern) because:

- Multi-process background tests get killed between tool calls if not done
  within one PowerShell invocation - keep an entire test's setup, commands,
  and teardown in one script.
- Prefer closing stdin (`proc.StandardInput.Close()`) to trigger the REPL's
  natural EOF-exit and reading via `StandardOutput.ReadToEndAsync()` after
  `WaitForExit()`, over async `OutputDataReceived` events - the latter has
  shown real ordering/completeness races in this project's tests (lines
  arriving out of order or not at all within the polling window). Killing a
  still-running process and hoping buffered output already flushed is
  similarly unreliable.
- `std::cout` is already set to `unitbuf` in `raft_main.cpp` specifically so
  redirected-stdout tests see output promptly - don't remove that.
- Scratch state dirs used for manual testing (`node1/`-`node3/`,
  `single_test/`, `txn_test/`) are gitignored - reuse or recreate them
  rather than inventing new ones per test unless there's a reason to.

## Git conventions

- Only commit when the user explicitly asks. Only push when explicitly
  asked (usually a separate ask after commit).
- Commit messages: explain *why*, reference the specific bug/mechanism
  fixed, note what was verified by hand. See recent commits for the
  established style/depth.
- `config/routing.conf` is gitignored (environment-specific, hand-edited per
  deployment) - `README.md`'s Phase 3 section documents the file format for
  users to write their own.
