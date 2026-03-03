# `br` Fork Plan: Replace `fsqlite` With `rusqlite`

## Purpose

This document is the durable implementation plan for our `beads_rust` fork.

The goal is to keep the `br` product model we want:

- local SQLite cache for speed
- Git-tracked JSONL for durability and collaboration
- stable CLI/workflow semantics

But we want to remove the current storage-engine risk:

- `beads_rust` currently depends on `fsqlite` / `frankensqlite`
- that engine is causing correctness failures in real write paths
- we want to move the default backend back to real SQLite via `rusqlite`

This plan is written so a future chat or a future agent can pick up the work
without needing to reconstruct the context from scratch.

## Current Execution Status (2026-03-03)

This document is now both the migration plan and the running implementation log.
It should be updated as the fork progresses so a future chat can resume work
without reconstructing the state manually.

### What Has Already Been Done In The Fork

Work is currently happening in:

- `/Users/stefanraath/Code/Studio/camera-app/_forks/beads_rust`
- branch: `rusqlite-default-backend`

The following backend-port work is already in place:

- Created a backend adapter seam at `src/storage/db.rs`
- Re-routed storage modules to import `Connection`, `DbError`, `Row`, and
  `SqliteValue` from that adapter instead of importing `fsqlite` directly
- Replaced the `fsqlite` crate family in `Cargo.toml` with:
  - `rusqlite = { version = "0.37", features = ["bundled"] }`
- Removed the `[patch.crates-io]` overrides that redirected the `fsqlite-*`
  family to `frankensqlite`
- Updated `src/error/mod.rs` so `BeadsError::Database` wraps the local
  adapter-level `DbError` rather than `FrankenError`
- Added a regression test for repeated blocked-cache rebuild mutations and
  integrity checks
- Verified targeted blocked-cache tests pass under the new `rusqlite` backend

### Current Open Work In The Fork

The main backend swap is already underway and compiling.

Completed in this session:

- removed the hardcoded `MAX_DEPTH=50` truncation in
  `rebuild_blocked_cache_impl`
- replaced it with a convergence-based bound tied to total issue count
- added a regression test that proves parent-child blocked propagation works
  beyond 50 levels
- re-ran targeted blocked-cache regressions after the change

The next correctness tasks after this are:

- broaden test coverage around multi-connection write contention using
  `rusqlite` semantics
- run a wider storage-focused test slice to catch any remaining adapter gaps
- continue simplifying `fsqlite`-specific compatibility code that is no longer
  needed after the backend swap

### Current Modified Files In The Fork

As of this note, the main worktree changes in the fork are:

- `Cargo.lock`
- `Cargo.toml`
- `src/cli/commands/delete.rs`
- `src/cli/commands/doctor.rs`
- `src/error/mod.rs`
- `src/storage/db.rs`
- `src/storage/events.rs`
- `src/storage/mod.rs`
- `src/storage/schema.rs`
- `src/storage/sqlite.rs`
- `tests/storage_invariants.rs`

## Additional Findings Captured During Implementation

### `fsqlite` vs `rusqlite`

One important clarification from direct inspection of the upstream source:

- `fsqlite` is not built on top of `rusqlite`
- `fsqlite` / `frankensqlite` is its own SQLite-style engine implemented in
  Rust (custom pager, WAL, B-tree, parser, planner, etc.)
- `rusqlite` appears in the `frankensqlite` workspace as a comparison /
  reference dependency, not as the runtime engine

This matters because moving `br` to `rusqlite` is not "changing wrappers."
It removes the custom storage engine from the runtime path and puts `br` back
on standard SQLite semantics.

### Meaning Of The `cursor must be on a leaf to delete` Failure

The `cursor must be on a leaf to delete` error is a storage-engine invariant
failure, not a bead-graph logic error.

In a SQLite-style B-tree:

- rows live on leaf pages
- delete operations are expected to act on a cursor positioned on a leaf

That error means the engine believed it was deleting through a cursor in a
context where the cursor was not positioned on a leaf node. In practical terms,
that points to a bug in the custom B-tree / cursor logic, or internal B-tree
state corruption in the engine.

This is exactly the class of bug we avoid by moving `br` back to real SQLite
through `rusqlite`.

### Why The `MAX_DEPTH=50` Cap Exists

The `MAX_DEPTH=50` limit in `rebuild_blocked_cache_impl` was introduced in the
first implementation of blocked-cache rebuild on January 16, 2026 (commit
`6d30f92`), before the later `fsqlite` migration. It was not added as an
`fsqlite` workaround.

It appears to have been a blunt defensive guard:

- prevent a feared infinite loop
- bound repeated transitive propagation work during the initial implementation

The current evidence indicates this cap was arbitrary rather than principled.

### Why The Infinite-Loop Concern Is Weak In This Specific Algorithm

The blocked-cache propagation loop is monotonic:

- each pass only inserts `parent-child` descendants that are not already in
  `blocked_issues_cache`
- once an issue is inserted, it is excluded from future passes
- the number of issues is finite

That means the loop already has a natural fixed point:

- it terminates when `newly_blocked.is_empty()`
- in the worst case it can only add each issue once

So the hardcoded `50` is not required for termination.

### What The `MAX_DEPTH` Warning Actually Means

If `rebuild_blocked_cache_impl` hits `depth >= MAX_DEPTH`:

- it logs a warning
- it breaks out of the propagation loop
- it still returns success

That means the warning is not just incidental logging. It is a real functional
correctness problem:

- the blocked cache is silently truncated
- deeper descendants are omitted
- commands such as `br ready` can return incorrect results for deep graphs

This is separate from the `fsqlite` corruption bug, but the two issues can be
triggered by the same write paths because blocked-cache rebuild runs during
status changes, dependency updates, sync operations, defer operations, and some
epic flows.

### `MAX_DEPTH` Fix Applied In This Fork

The fork now removes the hardcoded `50`-level cap.

The replacement behavior is:

- the propagation loop runs until no new blocked descendants are found
- the code computes a conservative iteration bound from the total number of
  issues in the database
- if that bound is ever exceeded, the rebuild returns an explicit error instead
  of logging a warning and silently returning a partial cache

This preserves termination guarantees while keeping dependency semantics
correct for deep graphs.

Regression coverage added:

- a targeted test builds a chain of 62 issues beneath a blocked root and
  asserts all 62 descendants land in `blocked_issues_cache`
- the test also verifies `ready` only returns the unblocked root issue and that
  `PRAGMA integrity_check` still returns `ok`

### Write-Contention Proof Added In This Fork

The fork now has a file-backed two-writer regression test covering the most
important operational concurrency guarantee for the `rusqlite` path:

- one connection opens `BEGIN IMMEDIATE` and holds the write lock
- a second connection attempts `create_issue`
- the second writer fails cleanly with a transient database lock/busy error
  after retrying, rather than corrupting the database
- once the first writer commits, the second writer can immediately retry and
  succeed
- `PRAGMA integrity_check` still returns `ok` afterward

This is the behavior we want from the backend:

- contention becomes a normal lock/retry condition
- not silent data loss
- not storage-engine corruption

### Broader Storage Parity Check (And A Real Regression We Caught)

After the targeted regressions passed, the fork ran a broader storage-focused
test slice:

- `storage_blocked_cache`
- `storage_crud`
- `storage_deps`
- `storage_ready`

This was useful because it surfaced a genuine adapter-port regression that the
custom tests did not cover:

- `upsert_issue_for_import` was still writing `NULL` into several
  `TEXT NOT NULL DEFAULT ''` issue columns (`description`, `design`, etc.)
- stock SQLite correctly rejected that with `NOT NULL constraint failed`

That failure was caused by a semantic mismatch in the port:

- the create path already normalized those fields to `""`
- the import-upsert path still used the older `NULL` behavior

The fix applied in the fork:

- `upsert_issue_for_import` now mirrors `create_issue` for the schema-default
  text fields
- it writes `""` (or `"."` for `source_repo`) where the schema expects
  non-null/default text values

After that fix:

- the upsert-focused `storage_crud` tests passed again
- the broader storage-focused suite returned green

This is exactly why the proof-driven approach matters: it already caught and
closed a real `rusqlite` parity gap before the fork was used as the daily
driver.

### Low-Risk `fsqlite` Workaround Cleanup Already Started

With the main `rusqlite` backend running and the higher-risk regressions covered,
the fork has started removing the safest `fsqlite`-specific workarounds first.

Completed in this pass:

- `dirty_issues` writes now use a real SQLite upsert keyed by `issue_id`
  instead of `DELETE + INSERT`
- `export_hashes` writes now use a real SQLite upsert keyed by `issue_id`
  instead of `DELETE + INSERT`
- `upsert_issue_for_import` now uses a true row upsert on `issues(id)`
  instead of deleting and recreating the issue row
- `config` and `metadata` are back to real keyed tables (`key PRIMARY KEY`)
  instead of the old non-unique `fsqlite` workaround layout
- `set_config`, `set_metadata`, and `set_metadata_in_tx` now use native SQLite
  upserts keyed by `key`
- `create_issue` no longer does a manual duplicate-ID probe before insert; it
  now relies on the `issues(id)` primary key atomically
- schema batch execution now delegates to real SQLite `execute_batch` instead
  of manually splitting SQL text on `;`
- `get_epic_counts` now uses a normal grouped SQL aggregate instead of fetching
  all child rows and aggregating in Rust
- `init` now writes a real `issue_prefix` entry into `.beads/config.yaml`
  instead of leaving the prefix commented out

Why these were safe to change first:

- both tables already have real primary keys in the schema
- the product semantics do not change
- the change reduces write churn and removes compatibility code that only
  existed because `fsqlite` could not be trusted to handle the classic upsert
  path cleanly

Why the duplicate-ID change matters:

- the old path introduced an extra read-before-write race window that existed
  only because `fsqlite` uniqueness could not be trusted
- on real SQLite, the primary key already enforces this atomically
- removing the pre-check makes `create_issue` simpler and moves correctness back
  to the database engine where it belongs

Validation after this change:

- `test_create_duplicate_id_fails` passed in the unit-test storage suite
- `create_duplicate_id_fails` passed in `tests/storage_crud.rs`

Why the `execute_batch` change matters:

- the old manual splitter was only present because `fsqlite` lacked batch
  execution
- splitting SQL on raw semicolons is semantically weaker than real SQLite batch
  parsing and can break valid SQL text that contains semicolons inside string
  literals
- delegating to SQLite removes that parser shim and aligns schema/migration
  execution with the real backend

Validation after this change:

- adapter test proved `execute_batch` correctly handles semicolons inside string
  literals
- `storage::events::tests::test_insert_created_event` passed
- `storage::schema::tests::test_migration_rebuilds_legacy_config_metadata_primary_keys`
  passed

Why the `get_epic_counts` change matters:

- the old Rust-side aggregation only existed because `fsqlite` could not handle
  `SUM(CASE ...)` correctly
- on real SQLite, grouped aggregation is the natural and cheaper implementation
- this reduces row materialization and moves the counting logic back into SQL

Validation after this change:

- `cli::commands::epic::tests::epic_status_tracks_children_and_eligibility`
  passed

Why the `init` config fix matters:

- `--no-db` mode resolves the issue prefix from `.beads/config.yaml` before it
  falls back to scanning JSONL
- previously, `init` wrote `issue_prefix` as a commented template line, which
  left no-db mode dependent on JSONL inference even in a freshly initialized
  workspace
- that made valid no-db workflows brittle as soon as JSONL inference became
  ambiguous

Validation after this change:

- `cli::commands::init::tests::test_init_with_prefix` now also verifies the
  generated `config.yaml` contains the real prefix entry
- `e2e_no_db_read_write` passed after fixing the test to use the workspace's
  actual configured prefix instead of a hardcoded `bd-*` ID

Current canary signal:

- focused critical e2e slice is green:
  - `e2e_basic_lifecycle`
  - `e2e_ready`
  - `e2e_epic`
  - `e2e_sync_artifacts`
  - `e2e_sync_preflight_integration`
- this is the strongest readiness signal so far that the fork is approaching
  day-to-day usability for real `br` workflows

Manual disposable-workspace canary also passed using the built `target/debug/br`
binary (not `cargo run`):

- `br init`
- `br create` (multiple issues)
- `br dep add`
- `br ready --json` (blocked issue correctly excluded)
- `br sync --status` (reported in sync)
- `br --no-db list --json` (loaded JSONL correctly with dependency counts)

This is the first direct proof that the forked binary is behaving correctly in a
real CLI workflow outside the test harness.

Validation after this cleanup:

- focused e2e coverage for `ready`, sync preflight, and JSONL import/export
  stayed green
- `storage_crud`, `storage_export_atomic`, and `jsonl_import_export` stayed
  green after the upsert change

Why the `upsert_issue_for_import` change matters:

- the old `DELETE + INSERT` path could cascade-delete related rows tied to the
  issue (`events`, `comments`, `labels`, etc.) before the import flow rebuilt
  some of them
- on real SQLite, that is both unnecessary and semantically risky
- the new row upsert preserves related rows that are not supposed to be blown
  away during issue import, especially audit history

Regression coverage added:

- an integration test now verifies that `upsert_issue_for_import` updates the
  issue row without deleting the existing audit events recorded for that issue

Why the `config` / `metadata` change matters:

- the old `fsqlite` path intentionally stripped uniqueness from those tables
  and forced the app to emulate replacement semantics with `DELETE + INSERT`
- that was a backend compromise, not a desirable database design
- on `rusqlite`, those tables can go back to their natural shape: one row per
  key, enforced by SQLite itself

Migration behavior added:

- legacy non-unique `config` / `metadata` tables are rebuilt to keyed tables
  during schema application
- if duplicate keys somehow exist, the migration keeps the latest row
  (highest `rowid`) for each key

Validation after this change:

- schema migration test for legacy `config` / `metadata` rebuild passed
- targeted storage test confirmed repeated `set_config` overwrites the existing
  key without creating duplicates
- `e2e_config_precedence` passed
- `e2e_beads_jsonl_env_overrides_metadata` passed

## Current Context

### Why We Are Doing This

We hit real `br` corruption/failure while building a large, dependency-heavy bead
graph for this repo.

Observed local behavior in this workspace:

- `br create` and some reads succeeded
- later writes failed with:
  - `Database error: internal error: cursor must be on a leaf to delete`
- `br doctor` reported schema anomalies (missing core tables) even while reads
  still partially worked
- `br sync --rebuild` also failed with the same storage error

This is not a failure of the "SQLite + JSONL" architecture.
It is a failure of the current SQLite-compatible engine underneath `br`.

### Important Local Safety Note

As of the last successful export in this repo:

- `.beads/issues.jsonl` is the safest source of truth
- `.beads/beads.db` should be treated as disposable cache state

If we resume using `br`, we should rebuild the DB from JSONL after the forked
binary is fixed.

## Upstream Findings

### 1. `beads_rust` Is Tightly Coupled To `frankensqlite`

`beads_rust` `main` currently depends on the full `fsqlite` family and patches
those crates to `Dicklesworthstone/frankensqlite` `main`.

Evidence:

- `Cargo.toml` includes `fsqlite`, `fsqlite-types`, `fsqlite-error`,
  `fsqlite-core`, `fsqlite-func`, `fsqlite-vdbe`, `fsqlite-vfs`,
  `fsqlite-pager`, `fsqlite-parser`, `fsqlite-planner`, `fsqlite-wal`,
  `fsqlite-btree`, `fsqlite-ast`, `fsqlite-mvcc`, `fsqlite-observability`
- `Cargo.toml` also uses `[patch.crates-io]` to override those crates from
  `https://github.com/Dicklesworthstone/frankensqlite`

Implication:

- this is not "use stock SQLite through Rust bindings"
- this is "use a Rust reimplementation of SQLite as the storage engine"

### 2. `SqliteStorage` Is Written Around `fsqlite` Semantics

The storage layer uses:

- `fsqlite::Connection`
- `fsqlite_types::SqliteValue`

It also contains explicit `fsqlite` workarounds:

- manual batch execution because `fsqlite` lacks `execute_batch`
- explicit duplicate checks because `fsqlite` does not reliably enforce some
  `UNIQUE` constraints
- `DELETE + INSERT` instead of upsert for dirty flags
- `FrankenError::BusySnapshot` retry handling
- schema/migration logic designed around `fsqlite` quirks

This means the migration is a real backend port, not just a dependency swap.

### 3. There Are Known Upstream Storage Bugs In The Exact Area We Hit

Relevant upstream issues:

- `#111`: `br sync --import-only` can write a DB with a corrupt page-count
  header, making it unreadable by standard SQLite. The issue report explicitly
  says `v0.1.13` (bundled `rusqlite`) did not have the bug, while `v0.1.14+`
  with `fsqlite` did.
- `#112`: exact failure we saw:
  `cursor must be on a leaf to delete`
- `#113`: `EXISTS` subqueries in `blocked_issues_cache` were not supported in
  `frankensqlite`, breaking update/close paths that rebuild the cache

The maintainer also appears to have landed fixes on `main` for some of these,
but that does not change the deeper risk:

- `br` is currently betting its local cache correctness on a young storage
  engine
- our use case stresses the exact code paths that are already known to be weak

### 4. `MAX_DEPTH=50` Is A Separate Functional Problem

`src/storage/sqlite.rs` currently hard-caps transitive blocked-cache propagation
at 50 levels during `rebuild_blocked_cache_impl`.

That is independent of the B-tree corruption bug.

Even after switching to `rusqlite`, we still need to fix this if we want deep,
accurate dependency graphs.

So our fork should do two things:

1. replace `fsqlite` with `rusqlite`
2. remove or redesign the `MAX_DEPTH=50` truncation

## Decision

We should keep using `br` as a product and workflow, but we should fork
`beads_rust` and move the default database backend to `rusqlite`.

We should **not** switch back to `bd` unless we need an immediate fallback.

The correct engineering move is:

- keep the CLI
- keep the JSONL format
- keep the `.beads` directory model
- keep the schema
- replace the local DB engine with real SQLite

This preserves the architecture we want while removing the most brittle layer.

## Non-Negotiable Constraints For The Fork

The first pass of the fork should preserve all of these:

- same `br` command surface
- same `.beads/issues.jsonl` format
- same `.beads/beads.db` file path
- same issue IDs and hierarchical child-ID behavior
- same schema (at least initially)
- same sync behavior
- same `AGENTS.md` integration model

The first pass should **not** attempt to redesign the entire app.

This must be a backend port first, not a rewrite.

## High-Level Strategy

Do this in two layers:

### Layer 1: Safe Storage Backend Swap

Replace `fsqlite` with `rusqlite` while keeping the public storage API and schema
as stable as possible.

This gets us back onto a production-proven database engine quickly.

### Layer 2: Remove `fsqlite`-Only Workarounds

After the port works and tests pass:

- remove compatibility hacks that only exist because of `fsqlite`
- simplify queries and migration logic
- fix deep dependency semantics (`MAX_DEPTH=50`)

This avoids mixing "stability restoration" with "cleanup and redesign."

## Recommended Implementation Plan

## Phase 0: Protect Existing Data And Establish A Baseline

Before touching the fork:

1. Treat all existing `beads.db` files created by current `br v0.1.20` as
   disposable caches.
2. Back up `.beads/issues.jsonl`.
3. Do **not** rely on old `beads.db` files as migration inputs.
4. Build recovery around JSONL import, not DB preservation.

For any repo already affected by the bug:

```bash
cp .beads/issues.jsonl .beads/issues.jsonl.backup
rm -f .beads/beads.db .beads/beads.db-wal .beads/beads.db-shm
```

We will only rebuild `beads.db` after the forked binary is fixed.

Also create a reproducible regression fixture:

- one JSONL file with 200+ issues
- one JSONL file with a deep parent-child chain > 50 levels
- one JSONL file with mixed blocking edges and status transitions

These fixtures should be checked into the fork's test suite.

## Phase 1: Fork The Repo And Create A Focused Migration Branch

The fork should start as a minimal, focused branch specifically for the storage
port.

Suggested branch name:

```bash
git checkout -b rusqlite-default-backend
```

Do **not** pile unrelated feature work into this branch.

The branch should answer one question first:

"Can `br` run the exact same product model on top of `rusqlite` reliably?"

## Phase 2: Lock In Regression Tests Before Changing The Backend

Before replacing dependencies, add tests that capture the failures we care about.

### Tests To Add First

1. Import JSONL with 250 issues and verify the resulting DB is readable by
   standard SQLite semantics.
2. Create/update/close/delete on a graph that triggers blocked-cache rebuild.
3. Rebuild blocked cache on a deep parent-child chain that exceeds 50 levels.
4. Run `ready` and verify transitive blockers are computed correctly for deep
   chains.
5. Rebuild from JSONL, then run a write that marks dirty issues and updates
   cache.

### What These Tests Protect

- issue `#111` class: invalid DB file format
- issue `#112` class: leaf-delete corruption during cache rebuild
- issue `#113` class: unsupported SQL in cache rebuild path
- our local workload: large dependency-heavy task graphs

### Important Testing Rule

These tests should be written to validate behavior, not `fsqlite` internals.

That way they remain valid after the backend swap.

## Phase 3: Replace Dependencies In `Cargo.toml`

This is the smallest safe dependency move:

### Remove

- all `fsqlite-*` dependencies
- the full `[patch.crates-io]` section pointing at `frankensqlite`

### Add

```toml
rusqlite = { version = "0.37", features = ["bundled"] }
```

### Why `bundled`

Because `br` is a CLI application, not a reusable generic library.

For a CLI, `bundled` is the right default because it gives:

- deterministic SQLite behavior across machines
- fewer system-library surprises
- less dependence on whatever SQLite happens to be installed on the host

Tradeoff:

- builds may be slightly heavier

That is acceptable. We care far more about correctness than shaving a little
build time.

## Phase 4: Add A Thin Database Adapter Layer Instead Of Rewriting All Query Code

This is the most important design choice in the fork.

Do **not** immediately rewrite every storage call site directly against raw
`rusqlite::Row`.

Instead, add a small compatibility layer, for example:

- `src/storage/db.rs`

Its job is to mimic the parts of the current `fsqlite` API shape that
`SqliteStorage` already expects.

### Why This Matters

Right now, the storage code is written around:

- `Connection`
- `query(...) -> rows`
- `query_row(...)`
- `execute_with_params(...)`
- `SqliteValue` extraction helpers

If we port directly to raw `rusqlite` everywhere, the migration becomes a large,
high-risk edit.

If we add an adapter, we make it mechanical.

### Recommended Adapter Shape

Create a small internal wrapper with owned values:

```rust
use rusqlite::{Connection, Params, types::{Value, ValueRef}};

pub type DbValue = Value;

pub fn open(path: &std::path::Path) -> crate::Result<Connection> { ... }
pub fn open_memory() -> crate::Result<Connection> { ... }
pub fn execute(conn: &Connection, sql: &str) -> crate::Result<usize> { ... }
pub fn execute_params<P: Params>(conn: &Connection, sql: &str, params: P) -> crate::Result<usize> { ... }
pub fn query_rows<P: Params>(conn: &Connection, sql: &str, params: P) -> crate::Result<Vec<Vec<DbValue>>> { ... }
pub fn query_row_opt<P: Params>(conn: &Connection, sql: &str, params: P) -> crate::Result<Option<Vec<DbValue>>> { ... }
```

Then add helper accessors for `DbValue`:

- `as_text`
- `as_integer`
- `as_real`
- `is_null`

This keeps most of `SqliteStorage` structurally similar to what it is now.

### Migration Principle

Use the adapter to get the fork stable first.

After that, if we want, we can later refactor toward more idiomatic direct
`rusqlite` usage.

## Phase 5: Port `src/storage/schema.rs`

This should be one of the easiest files to port.

### Keep

- `CURRENT_SCHEMA_VERSION`
- `SCHEMA_SQL`
- the current tables/indexes
- the same migration intent

### Replace

#### 1. `execute_batch`

Current code manually splits SQL on `;` because `fsqlite` does not support
`execute_batch`.

With `rusqlite`, use:

```rust
conn.execute_batch(sql)?;
```

This lets us remove a whole category of brittle compatibility logic.

#### 2. PRAGMAs

The schema setup can stay the same in meaning:

- `journal_mode = WAL`
- `foreign_keys = ON`
- `synchronous = NORMAL`
- `temp_store = MEMORY`
- `cache_size = -8000`
- `user_version`

But implement them using `rusqlite` primitives:

- `execute_batch`
- `pragma_update` where convenient

### First-Pass Rule

Do not change the schema in the first pass.

We want the backend port to be storage-engine-only, not schema-plus-backend.

## Phase 6: Port `src/storage/sqlite.rs`

This is the main work.

### A. Connection Opening

Replace:

- `fsqlite::Connection::open(...)`

With:

- `rusqlite::Connection::open(...)`
- `rusqlite::Connection::open_in_memory()`

And apply:

- `busy_timeout` if a lock timeout was requested

### B. Transactions

The current code uses:

- `BEGIN IMMEDIATE`
- `BEGIN EXCLUSIVE`
- manual `COMMIT` / `ROLLBACK`

With `rusqlite`, keep the same transaction semantics explicitly:

- use `TransactionBehavior::Immediate` for mutation paths
- use `TransactionBehavior::Exclusive` where the code truly needs exclusive
  rebuild behavior

Recommended pattern:

```rust
let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
// do writes
tx.commit()?;
```

This is clearer and safer than manually issuing SQL transaction commands.

### C. Query Execution

Port all storage queries through the adapter layer first.

That means:

- keep the higher-level storage logic intact
- only replace how rows are fetched and values are decoded

### D. Error Handling

Today the code explicitly handles `fsqlite_error::FrankenError`.

That must be removed.

Replace it with conversions from:

- `rusqlite::Error`

And where needed:

- inspect `Error::SqliteFailure(...)` codes for busy/locked conditions

### E. Retry Logic

Current code retries on:

- `FrankenError::BusySnapshot`

With `rusqlite`, we do not need to carry that exact logic forward unchanged.

Recommended approach:

- first pass: keep retry logic only for standard SQLite busy/locked conditions
- second pass: simplify further if tests show it is unnecessary

Do not preserve `fsqlite`-specific retry code just because it exists.

### F. Dirty Flag Writes

Current code uses:

- `DELETE FROM dirty_issues WHERE issue_id = ?`
- followed by `INSERT`

Reason given in code:

- `fsqlite` lacked reliable `UNIQUE` enforcement

With real SQLite, we can and should use an upsert.

Recommended replacement:

```sql
INSERT INTO dirty_issues (issue_id, marked_at)
VALUES (?, ?)
ON CONFLICT(issue_id) DO UPDATE SET
  marked_at = excluded.marked_at
```

This is simpler and removes extra mutation churn.

### G. Duplicate Checks

Current `create_issue` does an explicit duplicate check before insert because of
`fsqlite` behavior.

Recommended first pass:

- keep the explicit duplicate check temporarily to minimize semantic changes

Recommended second pass:

- remove it and rely on real `PRIMARY KEY` / `UNIQUE` enforcement

This keeps the port stable first, then cleans it up later.

## Phase 7: Remove `fsqlite`-Specific Schema Workarounds

After the port is green and regression tests pass, remove the code that exists
only because of `fsqlite`.

The biggest candidate is the "column order" rebuild logic in `schema.rs`.

The current file explicitly says this exists because `fsqlite` can fail with
"no such column" if column order differs after some migrations.

That is not a standard SQLite problem.

### Recommended Approach

1. Keep the code during the first port if needed for compatibility.
2. Add tests against real SQLite.
3. If those tests pass, delete the workaround.

This reduces maintenance burden and shrinks the amount of risky migration logic.

## Phase 8: Fix `blocked_issues_cache` Correctness For Deep Graphs

This is a separate fix, but it belongs in the same fork.

### Current Problem

`rebuild_blocked_cache_impl` uses:

- `const MAX_DEPTH: i32 = 50`

That means deeply nested parent-child chains can have truncated blocker
propagation, which is wrong for a dependency tracker.

### Recommended Fix

Remove the fixed depth cap and replace it with a termination rule based on actual
graph progress.

The current loop already computes `newly_blocked`.
That means the natural termination condition is:

- stop when `newly_blocked` is empty

To keep a safety guard without a hard semantic cutoff:

- compute `max_iterations = number_of_open_issues + 1`
- if exceeded, fail with a clear cycle/corruption-style error instead of silently
  truncating

That preserves safety without returning incomplete truth.

### Why This Is Safe

In a correct DAG:

- repeated expansion will converge

If it does not converge:

- that indicates a cycle or invariant violation

That should be surfaced as an error, not hidden behind silent truncation.

## Phase 9: Decide Whether To Keep `blocked_issues_cache` As A Materialized Table

This is optional for the first pass.

### Recommendation

Keep it for the initial migration.

Why:

- it minimizes behavior changes
- it keeps `ready` / `blocked` performance characteristics stable
- it isolates the backend port from a larger query redesign

### Later Improvement Option

After the fork is stable, evaluate either:

1. keeping the cache table but simplifying rebuild logic
2. replacing parts of it with recursive CTE-based runtime queries

Do **not** combine that redesign with the backend swap unless absolutely
necessary.

## Phase 10: Port The Error Layer

Find the file that defines `BeadsError` and database error conversions
(likely `src/error.rs` or `src/error/mod.rs`) and remove any direct dependency
on `fsqlite_error::FrankenError`.

### What To Change

- add `impl From<rusqlite::Error> for BeadsError`
- preserve the existing high-level `DATABASE_ERROR` behavior at the CLI surface
- keep machine-readable structured errors stable

### What To Avoid

Do not leak raw `rusqlite` enum internals into the CLI contract if the current
`br` UX expects a stable structured error model.

The backend should change. The user-facing error contract should remain as stable
as possible.

## Phase 11: Recovery Strategy For Existing Repos

Once the forked binary is ready, the safest migration path for existing repos is:

```bash
cp .beads/issues.jsonl .beads/issues.jsonl.backup
rm -f .beads/beads.db .beads/beads.db-wal .beads/beads.db-shm
br sync --import-only
```

Important:

- do not try to "repair" the old `fsqlite` DB in place
- rebuild from JSONL
- treat JSONL as canonical

This is consistent with the product design anyway: the DB is cache, JSONL is the
shared artifact.

## Recommended Commit Sequence In The Fork

Keep the migration reviewable.

Suggested sequence:

1. `test(regression): add import, cache rebuild, and deep dependency failure cases`
2. `chore(storage): replace fsqlite dependency stack with rusqlite`
3. `refactor(storage): add internal db adapter for rusqlite compatibility`
4. `refactor(storage): port schema and storage queries to rusqlite`
5. `fix(ready): remove MAX_DEPTH truncation from blocked cache rebuild`
6. `refactor(storage): replace dirty-flag delete+insert with upsert`
7. `refactor(storage): remove fsqlite-only schema workarounds`
8. `docs: document JSONL-first recovery from pre-rusqlite databases`

This makes it possible to bisect the migration cleanly.

## Exact Files Most Likely To Change

These are the primary targets we already know about:

- `Cargo.toml`
- `src/storage/mod.rs`
- `src/storage/schema.rs`
- `src/storage/sqlite.rs`

Probable additional files:

- the file that defines `BeadsError`
- tests around storage/import/export
- any module that assumes `fsqlite`-specific error types or value helpers

Likely new file:

- `src/storage/db.rs` (recommended adapter layer)

## Definition Of Done

We should consider the fork ready only when all of this is true:

1. `br init`, `br create`, `br update`, `br close`, `br delete`, `br dep add`,
   and `br sync` all work on a deep, dependency-heavy graph.
2. A DB imported from JSONL is readable by standard SQLite tools.
3. `ready` and `blocked` remain correct beyond 50 dependency levels.
4. Existing repos can recover by deleting `beads.db*` and importing from JSONL.
5. The CLI and JSONL behavior remain backward-compatible for normal users.

## What We Should Do In Practice

If we are actively implementing this fork, the next concrete move should be:

1. create the fork branch
2. add regression tests first
3. add the `rusqlite` adapter layer
4. port `schema.rs`
5. port `sqlite.rs`
6. fix the `MAX_DEPTH=50` blocker truncation
7. rebuild our local `camera-app` `.beads` DB from JSONL using the forked binary
8. resume granular bead creation only after the forked binary passes the tests

## Suggested Short-Term Policy For This Repo

Until the fork is ready:

- do not trust `br v0.1.20` for heavy write operations
- do not keep extending the current dependency graph with the released binary
- keep `.beads/issues.jsonl` as the authoritative snapshot

Once the fork is ready:

- rebuild `.beads/beads.db` from JSONL
- continue using `br`
- keep the same issue-tracking model we already committed to

## Reference Links

Primary upstream references used to form this plan:

- `beads_rust` crate docs: <https://docs.rs/beads_rust/latest/beads_rust/>
- `beads_rust` release page (`v0.1.20` latest as of 2026-03-03):
  <https://github.com/Dicklesworthstone/beads_rust/releases>
- `beads_rust` `Cargo.toml`:
  <https://raw.githubusercontent.com/Dicklesworthstone/beads_rust/main/Cargo.toml>
- `beads_rust` `src/storage/mod.rs`:
  <https://raw.githubusercontent.com/Dicklesworthstone/beads_rust/main/src/storage/mod.rs>
- `beads_rust` `src/storage/sqlite.rs`:
  <https://raw.githubusercontent.com/Dicklesworthstone/beads_rust/main/src/storage/sqlite.rs>
- `beads_rust` `src/storage/schema.rs`:
  <https://raw.githubusercontent.com/Dicklesworthstone/beads_rust/main/src/storage/schema.rs>
- `beads_rust` issue `#111`:
  <https://github.com/Dicklesworthstone/beads_rust/issues/111>
- `beads_rust` issue `#112`:
  <https://github.com/Dicklesworthstone/beads_rust/issues/112>
- `beads_rust` issue `#113`:
  <https://github.com/Dicklesworthstone/beads_rust/issues/113>
- `frankensqlite` README:
  <https://raw.githubusercontent.com/Dicklesworthstone/frankensqlite/main/README.md>
- `rusqlite` repository:
  <https://github.com/rusqlite/rusqlite>

## Final Recommendation

We should stay with the `br` product model, but not with `fsqlite` as the
default backend.

The right fork plan is:

- preserve the architecture
- swap the engine
- keep JSONL canonical
- fix deep dependency semantics
- rebuild local DBs from JSONL after the new binary is ready

That gives us the workflow we want without betting the tracker on an immature
storage engine.
