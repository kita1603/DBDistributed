# distdb

Distributed SQL database, built incrementally from scratch in C++ for learning
purposes. See the phase breakdown below for where this stands.

## Phase 1 (current): single-node storage engine

- `src/storage/wal.*` — append-only write-ahead log with per-record CRC32
  checksums, so a torn write at the tail (crash mid-append) is detected and
  discarded on replay instead of corrupting recovery. `Reset()` truncates it
  once its contents are durable in a flushed SSTable.
- `src/storage/memtable.*` — in-memory sorted map of the latest writes;
  deletes are stored as tombstones rather than removed, so they can shadow
  an older on-disk value for the same key in an SSTable.
- `src/storage/sstable.*` — immutable, sorted on-disk table: a data block,
  an index block (key -> offset, binary-searched by the reader), and a
  footer. `SSTableWriter` writes to `<path>.tmp` and only renames it to the
  final `.sst` path once fully flushed, so a crash mid-flush never leaves a
  half-written table that gets mistaken for a valid one.
- `src/storage/engine.*` — ties it together: every write hits the WAL then
  the memtable; once the memtable passes a size threshold it's flushed to a
  new SSTable and the WAL is truncated; once the SSTable count passes a
  threshold they're compacted into one (dropping tombstones, since a full
  compaction means nothing older is left that could still need them).
  `Get()` checks the memtable, then SSTables newest to oldest, stopping at
  the first layer with an answer.
- `src/main.cpp` — a REPL exposing both raw KV commands (`put`/`get`/`del`)
  and SQL statements.

### SQL layer (`src/sql/`)

Maps a small SQL subset onto the KV engine above — there's no query planner,
just a straight AST-walking executor:

- `lexer.*` — tokenizes SQL text (identifiers/keywords, numbers, quoted
  strings, symbols).
- `ast.h` — statement and expression types (`std::variant<CreateTableStatement,
  InsertStatement, SelectStatement, UpdateStatement, DeleteStatement>`).
- `parser.*` — recursive-descent parser for `CREATE TABLE`, `INSERT`,
  `SELECT`, `UPDATE`, `DELETE`. `WHERE` is limited to an AND-chain of
  `column <op> literal` comparisons — no `OR`, no subqueries, no `JOIN`.
- `schema.*` — a table's column list, persisted at key `__schema__/<table>`
  via the storage engine, so schemas survive restarts the same way rows do.
- `executor.*` — executes a parsed statement: rows are stored at key
  `__row__/<table>/<primary-key-value>`, encoded as length-prefixed fields
  (`EncodeRow`/`DecodeRow`) since column values can hold arbitrary bytes
  (unlike column names, which the schema's simpler delimited format assumes
  are plain identifiers). `SELECT`/`UPDATE`/`DELETE` scan every row under a
  table's key prefix via `StorageEngine::Scan` — there is no secondary
  indexing yet, so a filter on a non-primary-key column still reads the
  whole table.

Supported column types are `TEXT` and `INT` only; every table needs exactly
one `PRIMARY KEY` column; `INSERT` must specify every column (no `NULL`s,
no partial rows); updating the primary key column is rejected (it would
require moving the row to a new key, not just rewriting it in place).

### Raft layer (`src/raft/`) — Phase 2: election, heartbeats, and real log replication

A from-scratch Raft node: leader election, heartbeats, and a genuinely
replicated log - a client calls `Propose()` on whichever node is currently
leader, the command is appended to its log, replicated to followers with the
standard consistency check and conflict-truncation, and applied identically
on every node once a majority holds it durably.

- `types.h` — `NodeId`/`Term`/`LogIndex` aliases shared across the module.
- `message.*` — `RequestVote`/`AppendEntries` request/response structs
  (`AppendEntries` now carries real `LogEntry` batches, not just an empty
  heartbeat) and their binary encode/decode.
- `transport.*` — TCP RPC transport: one-shot connection per call, works on
  both Windows (Winsock2) and POSIX via `#ifdef`.
- `persistent_state.*` — `currentTerm`/`votedFor`, durable before replying to
  any RPC (otherwise a restarted node could vote twice in one term).
- `log.*` — the replicated log itself: 1-indexed `(term, command)` entries.
  `AppendEntriesFrom` implements the follower-side consistency check
  (`prevLogIndex`/`prevLogTerm` must match) and truncates any conflicting
  suffix before appending - entries that already match are left alone, so
  a retransmitted/duplicate `AppendEntries` is handled safely. Persisted by
  rewriting the whole log file on every mutation (same `.tmp`-then-rename
  crash safety as `SSTableWriter::Finish()`/`PersistentState::Set`) - simple
  and always crash-safe, at the cost of O(log size) work per append instead
  of O(1); fine at this project's scale.
- `node.*` — the state machine. Leaders track `nextIndex`/`matchIndex` per
  follower and, after each successful `AppendEntries`, recompute the
  majority-acknowledged index - only counting it as committed if that entry
  is from the leader's *own current term* (committing an older-term entry
  directly is exactly the unsafe case Raft's Figure 8 warns about). A
  follower's consistency-check failure backs `nextIndex` off by one and
  retries next round (linear backoff - simpler than jumping back by
  conflicting term, and still correct). `RequestVote` now does the real
  "candidate's log is at least as up-to-date as mine" check instead of a
  no-op. `Propose()` blocks until either a majority has replicated the
  entry or a timeout elapses, mirroring what a real client sees: an
  unconfirmed outcome should be retried, possibly against a new leader.
  `ProposeOrForward()` is the client-facing entry point that removes the
  need to retry manually: if this node isn't the leader, it forwards the
  request over the network (via a new `ClientRequest`/`ClientResponse` RPC)
  to whichever node it believes is, following up to a few redirects if that
  guess turns out to be stale too (e.g. leadership changed mid-request).
  Whatever node ends up handling the request commits it through the same
  `Propose()` used everywhere else - `ProposeOrForward` is purely about
  *finding* the leader, not a second way of committing. Because a
  `ClientRequest` can legitimately block for seconds waiting on a commit,
  `RaftTransport`'s accept loop spawns a thread per connection rather than
  handling requests inline (its original assumption - every RPC is short -
  no longer held once this one could block).
- `src/raft_main.cpp` → `raftnode.exe <id> <listen-port> <peers> [state-dir]`.
  Each node owns its own `StorageEngine` + `SqlExecutor` (the whole Phase 1
  stack) under `<state-dir>/kv`. The REPL takes SQL directly: a `SELECT` is
  read-only, so it runs immediately against local storage (not
  linearizable - a follower can be a little behind the leader); anything
  else (`CREATE TABLE`/`INSERT`/`UPDATE`/`DELETE`) is parsed just enough to
  confirm it's a write, then the *raw SQL text* goes through
  `ProposeOrForward()` - so a write typed into *any* node's REPL, follower
  or leader, just works. The `ApplyCallback` given to `RaftNode` just calls
  `SqlExecutor::Execute(command)` on whatever gets committed - every node
  runs the identical SQL statement against its own storage in the same
  order, which is what keeps them in sync. This is statement-based
  replication (each replica re-executes the SQL, rather than the leader
  shipping pre-computed row changes); it's simple and correct as long as
  statements are deterministic, which this project's SQL subset always is
  (no functions like `NOW()`/`RANDOM()` exist to violate that). A failing
  statement (e.g. a duplicate primary key) fails identically on every
  replica, so it never causes divergence - but `ApplyCallback` must still
  catch the exception itself (see the contract on `ApplyCallback` in
  `raft/node.h`): letting it escape would call `std::terminate()` on
  whichever detached RPC-response thread happened to be applying it.

Verified by hand (3 local processes): `CREATE TABLE` + `INSERT` sent directly
to a *follower's* REPL are transparently forwarded to the real leader and
come back `OK (committed at index N, via leader node <leader>)` - the row
then shows up via `SELECT` on the third, untouched node, confirming the
forwarded write actually replicated everywhere and wasn't just accepted
locally. Separately: killing the leader elects a new one that keeps the
already-committed entries (its log survives on disk); a second `INSERT`
proposed on the *new* leader also replicates, and the surviving follower's
final `SELECT` shows both the pre-crash and post-crash rows - a full write →
replicate → crash → re-elect → keep-writing cycle with no data loss.

#### Log compaction and InstallSnapshot

`RaftLog` tracks a `snapshot_index_`/`snapshot_term_` boundary in addition to
its entries. Once `last_applied_` passes that boundary by `kCompactionThreshold`
(5, small on purpose so it's easy to trigger by hand), `ApplyCommitted()` calls
`log_.CompactTo()`, which discards every entry up to that point - they're
already durably reflected in the state machine (`StorageEngine` persists
itself independently via its own WAL/SSTables), so nothing is lost, and the
`.tmp`-then-rename rewrite this keeps small stays cheap.

A follower whose `nextIndex` has fallen at or before the *leader's*
`snapshot_index()` can't be caught up via `AppendEntries` any more - those
entries don't exist there any more either. `ReplicateTo` detects this and
calls `SendInstallSnapshot` instead: the leader dumps its entire current
state via `SnapshotCallback` and ships it in one `InstallSnapshotRequest`;
the follower's `RestoreCallback` wipes its own state (`StorageEngine::Reset()`)
and reloads it wholesale, then adopts the same compaction boundary via
`log_.CompactTo()`.

Two correctness details that weren't obvious until testing surfaced them:

- **The declared boundary must exactly match what the dump contains.**
  `SnapshotCallback` is deliberately called *with `mutex_` held* (unlike
  `ApplyCallback`, which explicitly isn't), specifically so no commit can land
  between reading `last_applied_` and taking the dump - `log_.snapshot_index()`
  looked like the obvious index to use here and is wrong, since compaction only
  runs every `kCompactionThreshold` entries while the state machine is updated
  on every single one. Understating the boundary would make the follower
  later re-receive, and re-apply, entries whose effect the dump already
  contains - and unlike a raw KV `PUT`, replaying a SQL `INSERT` against data
  that's already there fails outright (duplicate primary key) rather than
  harmlessly doing nothing twice.
- **Two InstallSnapshot RPCs can race on the same follower.** The accept loop
  spawns a thread per connection, so if a leader's retry overlaps with a
  still-in-flight earlier attempt, two wholesale restores could run out of
  order and let a stale (smaller) snapshot clobber a newer one that already
  landed. A dedicated `install_mutex_` (not `mutex_` - a slow restore
  shouldn't block unrelated RPCs like elections) serializes
  `HandleInstallSnapshot` calls against each other so this can't happen.

Verified by hand: kill a follower immediately (before it receives anything),
write enough entries through the leader to trigger two compactions, revive
the follower fresh - `SendInstallSnapshot` fires automatically on the next
replication round, and the revived node's `SELECT` shows every row once
catch-up settles, with no duplicate-key apply errors.

Known simplifications: RPCs are one-shot connections, not persistent
per-peer links; peer addresses must be literal IPs (`SendRequest` uses
`inet_pton`, which doesn't resolve hostnames - no `getaddrinfo` yet); reads
(`get`/`SELECT`) aren't linearizable; election/heartbeat timing constants are
tuned for fast local testing, not production margins; there's no
authentication or encryption between nodes, fine for a trusted LAN/localhost
but not for anything reachable by untrusted hosts; `SendInstallSnapshot` has
no backoff, so a leader keeps rebuilding and resending a fresh dump on every
replication round to an unreachable/still-lagging peer instead of waiting
between attempts - wasteful but not incorrect at this project's scale;
calling `SnapshotCallback` with `mutex_` held means a large dump would block
the whole node's RPC handling for its duration - deliberately traded for
correctness here (see above), but would need addressing (e.g. a
copy-on-write/versioned state machine) at real data volumes.

`connect()` *is* bounded by `timeout_ms` (via a non-blocking connect +
`select()`, in `transport.cpp`) - this matters more than it sounds: on
localhost a dead port refuses a connection immediately, so a plain blocking
`connect()` would never visibly hang there. Across real machines, though, a
firewall that silently drops packets (instead of actively refusing them)
leaves a blocking `connect()` waiting on the OS's own default TCP connect
timeout - commonly tens of seconds - which from the caller's side just looks
like the whole command froze. Multi-machine testing is exactly where this
would otherwise bite.

Not yet implemented (later phases): sharding, distributed transactions.

Known simplifications (fine for learning, called out for later): `MaybeCompact`
merges every current SSTable at once rather than a leveled/tiered policy over
a subset; `SSTableReader::ReadAll`/`Scan` materialize results in memory
instead of streaming; flush/compaction thresholds
(`kMemtableFlushThreshold`, `kCompactionTriggerCount` in `engine.cpp`) are
tiny (4) so they're easy to trigger by hand instead of being sized for real
data volumes; SQL and raw KV commands share one key space, so a raw `put`
under `__row__/...`/`__schema__/...` could corrupt a table with no
validation stopping it.

## Build

Requires a C++17 compiler, CMake >= 3.16, and Ninja.

```
cmake -S . -B build -G Ninja
cmake --build build
```

Toolchain note: this project needs full C++17 standard library support
(`<optional>`, `<filesystem>`, non-const `std::string::data()`). Old MinGW.org
GCC builds (e.g. 6.x) don't have these — use a modern MinGW-w64 distribution
(this was set up with WinLibs GCC 16 via
`winget install BrechtSanders.WinLibs.POSIX.UCRT`) or MSYS2.

On MinGW, `CMakeLists.txt` links every executable statically
(`add_link_options(-static)`) so the built `.exe` runs standalone without
needing the toolchain's `bin` directory on `PATH`. This matters for the raft
executable specifically: tests launch several copies of it as independent
background processes, each in its own shell/session that may not have that
`PATH` entry.

## Run

```
build/distdb.exe [data-dir]
```

`data-dir` defaults to `./data` and holds `wal.log` plus any flushed
`*.sst` files. Data survives restarts:

```
> CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, age INT)
OK
> INSERT INTO users (id, name, age) VALUES ('u1', 'Alice', 30)
OK
> SELECT * FROM users WHERE age > 20
id	name	age
u1	Alice	30
> exit
```
```
> SELECT * FROM users
id	name	age
u1	Alice	30
```
