# Maintainer Handoff: Upstream `br` Findings From `fsqlite` / `frankensqlite` Validation

Date: 2026-03-08

Target upstream revision tested:

- repo: `https://github.com/Dicklesworthstone/beads_rust`
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

Control fork used for comparison:

- repo: `rustytroy/beads_rust`
- binary: `bx`
- purpose: control only, to distinguish upstream `br` failures from harness errors

This document is written as a maintainer handoff. It is intended to be factual,
specific, and easy to split into individual upstream issues. It separates:

- issues confirmed against the latest upstream `br`
- structural risk points still present in upstream code even where a black-box failure did not reproduce in this pass
- issue-ready writeups that can be pasted into GitHub issues

## Scope

This validation was based on the problem classes called out in `BR_RUSQLITE_FORK_PLAN.md`:

- storage-engine correctness failures
- blocked-cache correctness failures
- import/upsert correctness risks
- schema execution hazards caused by `fsqlite` limitations

The current upstream still depends on the `fsqlite` / `frankensqlite` stack:

- upstream dependency declaration: `Cargo.toml:47-61`
- upstream still patches those crates from `Dicklesworthstone/frankensqlite`

Relevant upstream code points:

- hardcoded blocked-cache propagation cap: `src/storage/sqlite.rs:1619-1725`
- import path uses `DELETE + INSERT`: `src/storage/sqlite.rs:4290-4298`
- schema batch executor splits SQL on `;`: `src/storage/schema.rs:207-219`

Relevant fork control points:

- convergence-based blocked-cache propagation: `src/storage/sqlite.rs:1560-1600`
- explicit regression for >50 parent-child levels: `src/storage/sqlite.rs:5142-5215`
- concurrent writer recovery regression: `src/storage/sqlite.rs:5218-5278`
- import upsert uses `ON CONFLICT(id) DO UPDATE`: `src/storage/sqlite.rs:4071-4129`
- regression for preserving events across import upsert: `tests/storage_crud.rs:676-701`
- regression for semicolons inside SQL string literals: `src/storage/db.rs:381-390`

## Summary

Confirmed current upstream issues:

1. Deep parent-child blocked propagation still truncates at 50 levels.
2. Upstream concurrency suite currently fails with missing writes and WAL corruption.
3. A `.beads/beads.db` created by latest upstream `br` is not readable by standard SQLite, even when `br doctor` reports integrity `ok`.

Structural risk points still present upstream:

4. Import upsert still uses `DELETE + INSERT` because of `fsqlite` limitations.
5. Schema batch execution still splits SQL on `;` because `fsqlite` lacks batch execution support.

Non-reproductions in this validation:

- the multiple-parent blocked-cache repro passed
- repeated close/reopen blocked-cache mutation stress passed in black-box CLI form
- a simple JSONL import round-trip did update the issue and preserve comments in the tested case

That means the current upstream is not failing everywhere. The failures are specific, but they are still material.

## Reproduction Setup

```bash
git clone https://github.com/Dicklesworthstone/beads_rust.git /tmp/beads_rust_upstream
cd /tmp/beads_rust_upstream
git rev-parse HEAD
cargo build --release --bin br

BR=/tmp/beads_rust_upstream/target/release/br
```

Optional control build:

```bash
git clone https://github.com/rustytroy/beads_rust.git /tmp/beads_rust_bx
cd /tmp/beads_rust_bx
cargo build --release --bin bx

BX=/tmp/beads_rust_bx/target/release/bx
```

Helper requirements used in these reproductions:

- `jq`
- `python3`
- macOS or Linux shell environment

## Finding 1: Blocked Cache Truncates Beyond 50 Parent-Child Levels

Status: confirmed on latest upstream `br`

### Why This Matters

Upstream still hardcodes `MAX_DEPTH = 50` in transitive blocked-cache propagation:

- `src/storage/sqlite.rs:1619-1725`

That means `br ready` and `br blocked` can silently return incorrect results for deeper parent-child chains.

### Minimal Reproduction

```bash
tmp=$(mktemp -d /tmp/br-deep-chain.XXXXXX)
cd "$tmp"
git init -q
"$BR" init --prefix tst >/dev/null

root=$("$BR" create "Root blocker" --json | jq -r .id)

for i in $(seq 0 61); do
  id=$("$BR" create "Deep $i" --json | jq -r .id)
  eval "chain_$i=$id"
done

"$BR" dep add "$chain_0" "$root" --type blocks >/dev/null

for i in $(seq 1 61); do
  prev=$((i - 1))
  eval "child=\$chain_$i"
  eval "parent=\$chain_$prev"
  "$BR" dep add "$child" "$parent" --type parent-child >/dev/null
done

"$BR" blocked --limit 0 --json | jq 'length'
"$BR" ready --limit 0 --json | jq 'map(.id)'
"$BR" doctor --json | jq '.checks[] | select(.name == "sqlite.integrity_check")'
```

### Observed Result On Latest Upstream

- blocked count: `51`
- ready count: `12`
- deepest descendant was not blocked
- `doctor` still reported integrity `ok`

Observed control result on `bx`:

- blocked count: `62`
- ready count: `1`
- only the root blocker remained ready

### Expected Result

- all 62 descendants should be blocked
- only the root blocker should remain ready
- behavior should not silently truncate based on graph depth

### Likely Cause

The propagation loop stops at an arbitrary depth instead of running to a fixed point:

- `src/storage/sqlite.rs:1619`
- `src/storage/sqlite.rs:1720-1725`

### Suggested Fix Direction

- remove the hardcoded `MAX_DEPTH=50`
- use convergence-based termination instead
- fail loudly if a safety bound is exceeded, rather than returning a partial blocked cache

One working implementation of that approach exists in the fork:

- `src/storage/sqlite.rs:1560-1600`

### Suggested Upstream Issue

Title:

`blocked_issues_cache propagation silently truncates after 50 parent-child levels`

Suggested body:

```md
## Summary

Latest upstream `br` still silently truncates blocked-cache propagation after 50 parent-child levels.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

Create a root blocker, then a 62-issue parent-child chain under it, with the first child blocked by the root.

Expected:
- all 62 descendants appear in `br blocked`
- only the root blocker appears in `br ready`

Actual:
- only 51 issues appear blocked
- multiple descendants incorrectly appear in `br ready`

## Relevant Code

- `src/storage/sqlite.rs:1619-1725`

## Acceptance Criteria

- no arbitrary depth truncation
- blocked propagation converges to a fixed point
- regression test covers >50 parent-child levels
```

## Finding 2: Upstream Concurrency Suite Fails On Latest `br`

Status: confirmed on latest upstream `br`

### Why This Matters

This is the strongest direct evidence of current storage/runtime instability in upstream `br`.

### Reproduction

```bash
cd /tmp/beads_rust_upstream
cargo test --test e2e_concurrency --release -- --nocapture
```

### Observed Result On Latest Upstream

The test target failed with 3 failing tests:

- `e2e_concurrent_writes_succeed_with_retry`
- `e2e_write_serialization`
- `e2e_mixed_read_write_concurrency`

Observed failure messages included:

- `missing issue from thread 1`
- `missing serialized issue 0`
- `Database error: WAL file is corrupt: short read at frame 4: got 0, need 4120`

Test assertion sites:

- `tests/e2e_concurrency.rs:142-149`
- `tests/e2e_concurrency.rs:401-404`
- `tests/e2e_concurrency.rs:472-486`

### Expected Result

- concurrent writes should either serialize cleanly or fail with a clean busy/lock error
- mixed reads and writes should not corrupt WAL state
- no created issue should disappear after a successful write path

### Likely Cause

The failures are consistent with storage-engine concurrency or WAL handling problems, not merely CLI output issues.

This aligns with the broader `fsqlite` / `frankensqlite` risk profile already discussed in the fork plan.

### Suggested Fix Direction

- stabilize the runtime storage engine under concurrent read/write load
- ensure WAL behavior is compatible with standard SQLite semantics
- use a targeted concurrency regression suite as a gating signal before release

The fork already has a focused regression covering clean concurrent-writer failure/recovery:

- `src/storage/sqlite.rs:5218-5278`

### Suggested Upstream Issue

Title:

`latest br fails e2e_concurrency with missing writes and WAL corruption`

Suggested body:

```md
## Summary

Latest upstream `br` fails `tests/e2e_concurrency.rs` under release-mode test execution.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

```bash
cargo test --test e2e_concurrency --release -- --nocapture
```

## Failing Tests

- `e2e_concurrent_writes_succeed_with_retry`
- `e2e_write_serialization`
- `e2e_mixed_read_write_concurrency`

## Observed Errors

- `missing issue from thread 1`
- `missing serialized issue 0`
- `Database error: WAL file is corrupt: short read at frame 4: got 0, need 4120`

## Relevant Assertions

- `tests/e2e_concurrency.rs:142-149`
- `tests/e2e_concurrency.rs:401-404`
- `tests/e2e_concurrency.rs:472-486`

## Acceptance Criteria

- all concurrency tests pass under `--release`
- no WAL corruption under mixed read/write load
- no silent loss of committed writes
```

## Finding 3: Upstream-Created `.beads/beads.db` Is Not Readable By Standard SQLite

Status: confirmed on latest upstream `br`

### Why This Matters

Upstream presents the DB as SQLite-backed, and the file header begins with `SQLite format 3`, but the file is not readable by the standard SQLite library in this validation.

This creates a serious mismatch between:

- `br doctor`, which reports `sqlite.integrity_check = ok`
- standard SQLite tooling, which reports the DB as malformed

### Reproduction

```bash
tmp=$(mktemp -d /tmp/br-sqlite-compat.XXXXXX)
cd "$tmp"
git init -q
"$BR" init --prefix tst >/dev/null
"$BR" create "Probe" --json >/dev/null
"$BR" doctor --json | jq '.checks[] | select(.name == "sqlite.integrity_check")'

python3 - <<'PY'
import glob, os, sqlite3
db = glob.glob(".beads/*.db")[0]
print("db:", db)
conn = sqlite3.connect(db)
print(conn.execute("select id, title from issues").fetchall())
print(conn.execute("pragma integrity_check").fetchall())
conn.close()
PY
```

### Observed Result On Latest Upstream

- `br doctor` reported `sqlite.integrity_check` as `ok`
- Python `sqlite3` failed with:

`DatabaseError('database disk image is malformed')`

The file header still begins with:

`SQLite format 3`

### Expected Result

- a database file created by `br` should be readable by standard SQLite tooling
- `doctor` and standard SQLite should not disagree about basic integrity

### Likely Cause

This strongly suggests that the upstream runtime is not maintaining full on-disk compatibility with standard SQLite semantics, even if it writes an apparently SQLite-shaped file.

### Suggested Fix Direction

- validate the produced DB with standard SQLite, not only the internal engine
- add a regression that opens a freshly created `.beads/beads.db` with `sqlite3` / `rusqlite` and runs a trivial query plus `PRAGMA integrity_check`

The fork currently passes this exact cross-check under standard SQLite.

### Suggested Upstream Issue

Title:

`br-created beads.db reports ok in doctor but is malformed to standard sqlite3`

Suggested body:

```md
## Summary

A `.beads/beads.db` created by latest upstream `br` is not readable by standard SQLite, even though `br doctor` reports `sqlite.integrity_check = ok`.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

1. Run `br init`
2. Run `br create`
3. Run `br doctor --json`
4. Open `.beads/beads.db` with Python `sqlite3` or standard SQLite CLI

## Actual

- `br doctor` reports integrity `ok`
- standard SQLite reports `database disk image is malformed`

## Expected

- the DB file should be readable by standard SQLite
- integrity checks should agree

## Acceptance Criteria

- a DB created by `br` can be opened by standard SQLite tooling
- `select` queries work
- `PRAGMA integrity_check` returns `ok`
```

## Finding 4: Import Upsert Still Uses `DELETE + INSERT`

Status: structural risk confirmed in code, not reproduced as a user-visible failure in this run

### Why This Matters

Upstream still uses `DELETE + INSERT` inside `upsert_issue_for_import`:

- `src/storage/sqlite.rs:4290-4298`

The comment explicitly says this is because `fsqlite` does not enforce UNIQUE constraints on non-rowid columns.

This is important because the import path is on the sync hot path, and `DELETE + INSERT` is a much weaker primitive than a real keyed upsert.

### Current Validation Result

A simple black-box JSONL import round-trip did succeed on latest upstream:

- title updated correctly after `sync --import-only`
- comments were still visible after import

So this was not reproduced as a current end-user breakage in the simple tested case.

### Why It Is Still Worth Filing

- it is a storage-engine-driven workaround still present in a critical path
- it diverges from normal SQLite semantics
- it is exactly the kind of workaround that makes import behavior harder to reason about

### Suggested Fix Direction

- replace `DELETE + INSERT` with a true keyed upsert on `issues(id)`
- add a regression that proves import upsert preserves related rows such as audit events

The fork already implements this using `ON CONFLICT(id) DO UPDATE`:

- `src/storage/sqlite.rs:4071-4129`

The fork also has an explicit preservation test:

- `tests/storage_crud.rs:676-701`

### Suggested Upstream Issue

Title:

`import path still uses DELETE + INSERT instead of a true keyed upsert`

Suggested body:

```md
## Summary

`upsert_issue_for_import` still performs `DELETE + INSERT` because of `fsqlite` limitations.

## Relevant Code

- `src/storage/sqlite.rs:4290-4298`

## Why This Matters

- import/sync is a critical path
- delete/reinsert semantics are weaker than a true upsert
- this creates avoidable correctness risk and complexity

## Suggested Acceptance Criteria

- import path uses keyed upsert on `issues(id)`
- regression test verifies related rows are preserved across import upsert
```

## Finding 5: Schema Batch Execution Still Splits SQL On Semicolons

Status: structural risk confirmed in code, not reproduced as an active CLI failure in this run

### Why This Matters

Upstream still implements its own semicolon splitter:

- `src/storage/schema.rs:207-219`

The comment explicitly says this is because `fsqlite` does not support `execute_batch`.

This is a known hazard because SQL string literals can legally contain semicolons.

### Current Validation Result

This did not reproduce as a black-box CLI failure in this pass, but the upstream implementation is still structurally fragile.

The fork now has an explicit regression test for the semicolon-in-string case:

- `src/storage/db.rs:381-390`

That regression passes on the fork.

### Suggested Fix Direction

- use real batch execution semantics
- add a regression that includes semicolons inside SQL string literals

### Suggested Upstream Issue

Title:

`schema batch executor still splits SQL on semicolons due to fsqlite limitation`

Suggested body:

```md
## Summary

The schema batch executor still manually splits SQL on `;` because `fsqlite` does not support batch execution.

## Relevant Code

- `src/storage/schema.rs:207-219`

## Risk

This is structurally unsafe when SQL string literals contain semicolons.

## Acceptance Criteria

- schema execution uses real batch semantics
- regression test covers semicolons inside SQL string literals
```

## Non-Issues From This Validation

These are worth recording so issue filing stays honest and specific.

### Multiple-Parent Blocked Cache Repro

This passed on latest upstream:

```bash
cargo test --test repro_cache_crash --release -- --nocapture
```

Relevant test:

- `tests/repro_cache_crash.rs:50-102`

Conclusion:

- do not file this specific multiple-parent crash as a currently failing regression without new evidence

### Black-Box Mutation Stress

Repeated close/reopen cycles in a temp repo did not produce a visible integrity failure in the tested CLI scenario.

Conclusion:

- this is not currently the strongest upstream issue to lead with

### Simple JSONL Import Round-Trip

A simple forced JSONL update followed by `sync --import-only` did update the issue and preserve comments in the tested case.

Conclusion:

- import correctness concerns are still real at the implementation level
- but the issue should be framed as structural risk and technical debt, not as a reproduced end-user failure from this specific run

## Recommended Issue Filing Order

If these are filed as separate upstream issues, the highest-signal order is:

1. blocked-cache truncation beyond 50 levels
2. failing concurrency suite and WAL corruption
3. standard SQLite unreadable DB despite `doctor` reporting integrity `ok`
4. import path still using `DELETE + INSERT`
5. schema batch executor splitting on semicolons

## Suggested Issue Template

Use this structure for each issue:

```md
## Summary

One-paragraph statement of the problem.

## Version / Commit

- commit:
- describe:
- platform:

## Reproduction

Copy-pasteable commands only.

## Expected

- ...

## Actual

- ...

## Relevant Code

- path:line-line

## Notes

- whether this is a reproduced runtime failure or a structural code risk
- any related tests or existing comments in the code

## Acceptance Criteria

- concrete, testable completion conditions
```

## Bottom Line

The latest upstream `br` still shows active, reproducible problems in the two areas that mattered most in the fork plan:

- deep blocked-cache correctness
- concurrency / WAL behavior

There is also a strong compatibility signal that upstream-created `.beads/beads.db` files are not behaving like standard SQLite databases under external inspection.

The remaining import and schema findings are more appropriately described as structural technical debt still forced by `fsqlite` limitations, rather than freshly reproduced user-visible failures in this pass.
