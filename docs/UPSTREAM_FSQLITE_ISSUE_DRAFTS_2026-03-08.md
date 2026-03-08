# Upstream Issue Drafts

Date: 2026-03-08

Use this file after sharing the maintainer handoff:

- handoff doc: `docs/UPSTREAM_FSQLITE_FINDINGS_2026-03-08.md`

Recommended sequence:

1. Share the maintainer handoff privately first so the maintainer has context.
2. Open the issues below one at a time in priority order.
3. Link each issue back to the handoff doc.

Tested upstream revision:

- repo: `https://github.com/Dicklesworthstone/beads_rust`
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Issue 1

Title:

`blocked_issues_cache propagation silently truncates after 50 parent-child levels`

Body:

```md
## Summary

Latest upstream `br` still silently truncates blocked-cache propagation after 50 parent-child levels.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Why This Matters

This causes `br blocked` and `br ready` to return incorrect results for deeper dependency hierarchies. The behavior is not a hard failure; it returns a partial blocked cache and makes some descendants incorrectly appear ready.

## Reproduction

```bash
git clone https://github.com/Dicklesworthstone/beads_rust.git /tmp/beads_rust_upstream
cd /tmp/beads_rust_upstream
cargo build --release --bin br

BR=/tmp/beads_rust_upstream/target/release/br

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
```

## Expected

- all 62 descendants appear in `br blocked`
- only the root blocker appears in `br ready`

## Actual

- only 51 issues appeared blocked
- multiple descendants incorrectly appeared in `br ready`

## Normal Use Trigger

This can happen in ordinary use when a project has a deep parent-child hierarchy, for example:

- an epic with many nested subtasks
- a planning tree where parent-child relationships are used across many levels
- repeated use of `br create --parent ...` or `br dep add ... --type parent-child`

The issue does not require concurrency. A single user in a single shell can trigger it by building a sufficiently deep hierarchy.

## User-Visible Symptom

- `br ready` shows deeply nested work as actionable when it is still transitively blocked
- `br blocked` under-reports blocked descendants
- teams/agents using `ready` as the source of truth can start work out of dependency order

## Affected Commands

- `br ready`
- `br blocked`
- `br dep add`
- `br create --parent`
- any command that triggers blocked-cache rebuild after dependency or status changes

## Severity / Operational Impact

This is a correctness bug in normal scheduling behavior. It is silent, which makes it worse operationally: the command succeeds and returns plausible-looking output, but the output is wrong for deep hierarchies.

## Relevant Code

- `src/storage/sqlite.rs:1619-1725`

There is still a hardcoded `MAX_DEPTH = 50` in transitive blocked-cache propagation.

## Suggested Fix Direction

- remove the arbitrary depth cap
- propagate until convergence instead
- if a safety bound is needed, fail loudly rather than returning a partial blocked cache

## Acceptance Criteria

- no arbitrary depth truncation
- blocked propagation converges to a fixed point
- regression test covers >50 parent-child levels
```

## Issue 2

Title:

`latest br fails e2e_concurrency with missing writes and WAL corruption`

Body:

```md
## Summary

Latest upstream `br` fails `tests/e2e_concurrency.rs` under release-mode test execution.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

```bash
git clone https://github.com/Dicklesworthstone/beads_rust.git /tmp/beads_rust_upstream
cd /tmp/beads_rust_upstream
cargo test --test e2e_concurrency --release -- --nocapture
```

## Actual

The test target failed with these cases:

- `e2e_concurrent_writes_succeed_with_retry`
- `e2e_write_serialization`
- `e2e_mixed_read_write_concurrency`

Observed failure messages included:

- `missing issue from thread 1`
- `missing serialized issue 0`
- `Database error: WAL file is corrupt: short read at frame 4: got 0, need 4120`

## Expected

- concurrent writes should either serialize cleanly or fail with a clean busy/lock error
- mixed reads and writes should not corrupt WAL state
- no created issue should disappear after a successful write path

## Normal Use Trigger

This looks reachable during normal multi-process or multi-agent use of `br`, especially when multiple shells or agents operate on the same repo concurrently.

The most likely trigger patterns are:

- two terminals both running `br create`
- one agent doing `br update` / `br close` while another runs `br list` / `br ready`
- overlapping write-heavy operations such as `create`, `update`, `close`, `reopen`, or `dep add`
- any workflow where multiple tools point at the same `.beads/beads.db`

This is particularly relevant for agent-heavy workflows where multiple automated actors operate on the same working copy.

## User-Visible Symptom

- a newly created or updated issue does not appear afterward
- serialized write expectations fail and some writes appear to vanish
- mixed read/write activity produces DB or WAL corruption
- later reads return storage errors rather than clean lock/busy behavior

## Affected Commands

Likely affected write paths:

- `br create`
- `br update`
- `br close`
- `br reopen`
- `br dep add`
- `br dep remove`
- potentially `br sync --import-only`

Likely affected read paths during overlap:

- `br list`
- `br ready`
- `br blocked`
- `br show`
- `br doctor`

## Severity / Operational Impact

This is high severity because it is not just a stale-read or retry problem. The observed failures include missing writes and WAL corruption, which directly affect data integrity and confidence in concurrent use.

## Relevant Assertions

- `tests/e2e_concurrency.rs:142-149`
- `tests/e2e_concurrency.rs:401-404`
- `tests/e2e_concurrency.rs:472-486`

## Notes

This looks like a runtime storage/WAL correctness problem, not just a flaky CLI-output test. The WAL corruption message is especially concerning.

## Acceptance Criteria

- all `e2e_concurrency` tests pass under `--release`
- no WAL corruption under mixed read/write load
- no silent loss of committed writes
```

## Issue 3

Title:

`br-created beads.db reports ok in doctor but is malformed to standard sqlite3`

Body:

```md
## Summary

A `.beads/beads.db` created by latest upstream `br` is not readable by standard SQLite, even though `br doctor` reports `sqlite.integrity_check = ok`.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

```bash
git clone https://github.com/Dicklesworthstone/beads_rust.git /tmp/beads_rust_upstream
cd /tmp/beads_rust_upstream
cargo build --release --bin br

BR=/tmp/beads_rust_upstream/target/release/br

tmp=$(mktemp -d /tmp/br-sqlite-compat.XXXXXX)
cd "$tmp"
git init -q
"$BR" init --prefix tst >/dev/null
"$BR" create "Probe" --json >/dev/null
"$BR" doctor --json | jq '.checks[] | select(.name == "sqlite.integrity_check")'

python3 - <<'PY'
import glob, sqlite3
db = glob.glob('.beads/*.db')[0]
conn = sqlite3.connect(db)
print(conn.execute('select id, title from issues').fetchall())
print(conn.execute('pragma integrity_check').fetchall())
conn.close()
PY
```

## Actual

- `br doctor` reported `sqlite.integrity_check = ok`
- Python `sqlite3` raised `DatabaseError('database disk image is malformed')`

## Expected

- a DB created by `br` should be readable by standard SQLite tooling
- internal integrity reporting and standard SQLite should agree

## Normal Use Trigger

This is not a special edge-case workflow. It can arise in normal use as soon as a user:

- runs `br init`
- creates one or more issues
- later inspects `.beads/beads.db` with standard SQLite tooling

This matters anywhere the project expects interoperability with:

- `sqlite3`
- Python `sqlite3`
- external SQLite inspection tools
- third-party automation expecting standard SQLite semantics

## User-Visible Symptom

- external SQLite tools fail to open the DB
- scripts using standard SQLite bindings cannot inspect or validate `.beads/beads.db`
- users see a mismatch between `br doctor` and external tooling

## Affected Commands

Commands that create or mutate the DB are effectively implicated:

- `br init`
- `br create`
- `br update`
- `br close`
- `br reopen`
- `br dep add`
- `br sync`

The visible symptom often appears when using tools outside `br`, not necessarily while running `br` itself.

## Severity / Operational Impact

This is high severity from an interoperability and trust standpoint. Even if `br` itself can continue using the DB, users cannot rely on ordinary SQLite tooling to inspect or validate it.

## Notes

The database file still begins with an `SQLite format 3` header, which makes the mismatch more concerning.

## Acceptance Criteria

- a DB created by `br` can be opened by standard SQLite tooling
- trivial `SELECT` queries work
- `PRAGMA integrity_check` returns `ok`
```

## Issue 4

Title:

`schema batch executor still splits SQL on semicolons due to fsqlite limitation`

Body:

```md
## Summary

The schema batch executor still manually splits SQL on `;` because `fsqlite` does not support batch execution.

Tested on:
- commit: `02a75ec0d4bd3fa61f07ef57ccae228afa93b262`
- describe: `v0.1.23-3-g02a75ec`

## Reproduction

This was reproduced directly against the same execution shape upstream currently uses in `src/storage/schema.rs`.

To inspect the current implementation:

```bash
git clone https://github.com/Dicklesworthstone/beads_rust.git /tmp/beads_rust_upstream
cd /tmp/beads_rust_upstream
sed -n '207,219p' src/storage/schema.rs
```

Then run this disposable harness:

```bash
tmp=$(mktemp -d /tmp/issue5-semicolon-harness.XXXXXX)
cd "$tmp"

cat > Cargo.toml <<'EOF'
[package]
name = "issue5_semicolon_harness"
version = "0.1.0"
edition = "2021"

[dependencies]
fsqlite = "0.1.1"
EOF

mkdir -p src
cat > src/main.rs <<'EOF'
use fsqlite::Connection;

fn execute_batch_like_upstream(conn: &Connection, sql: &str) -> Result<(), String> {
    for stmt in sql.split(';') {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            conn.execute(trimmed).map_err(|err| format!("{err}"))?;
        }
    }
    Ok(())
}

fn run_case(name: &str, setup: &[&str], sql: &str) {
    let conn = Connection::open(":memory:").expect("open in-memory fsqlite db");
    for stmt in setup {
        conn.execute(stmt).expect("setup should succeed");
    }

    let fragments: Vec<&str> = sql
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    let result = execute_batch_like_upstream(&conn, sql);

    println!("CASE: {name}");
    println!("SQL: {sql}");
    println!("FRAGMENTS:");
    for (idx, fragment) in fragments.iter().enumerate() {
        println!("  {}. {}", idx + 1, fragment);
    }
    println!("RESULT: {:?}", result);
    println!();
}

fn main() {
    run_case(
        "insert literal with semicolon",
        &["CREATE TABLE demo(value TEXT NOT NULL)"],
        "INSERT INTO demo(value) VALUES('a;b'); INSERT INTO demo(value) VALUES('c');",
    );

    run_case(
        "create table default literal with semicolon",
        &[],
        "CREATE TABLE defaults_demo(value TEXT DEFAULT 'a;b');",
    );

    run_case(
        "create view literal with semicolon",
        &[],
        "CREATE VIEW demo_view AS SELECT 'a;b' AS value;",
    );
}
EOF

rustup run nightly-2026-02-19 cargo run --quiet
```

Observed failure cases:

- `INSERT INTO demo(value) VALUES('a;b'); INSERT INTO demo(value) VALUES('c');`
- `CREATE TABLE defaults_demo(value TEXT DEFAULT 'a;b');`
- `CREATE VIEW demo_view AS SELECT 'a;b' AS value;`

Observed behavior:

- the raw `split(';')` logic breaks the SQL inside the string literal
- `fsqlite` then raises an unterminated-string parse error
- example fragment output:
  - `INSERT INTO demo(value) VALUES('a`
  - `b')`

Example error:

- `unexpected token in expression: Error("unterminated string literal starting at byte ...")`

## Relevant Code

- `src/storage/schema.rs:207-219`

## Why This Matters

Manual semicolon splitting is structurally unsafe when SQL string literals contain semicolons.

## Expected

- schema and migration SQL should be executed using real batch semantics
- semicolons inside SQL string literals should not change statement boundaries
- schema application should be atomic at the statement-batch level the backend supports

## Actual

- upstream currently tokenizes schema SQL by splitting on raw `;`
- this fails for valid SQL containing semicolons inside string literals
- reproduced examples include:
  - `INSERT ... VALUES('a;b')`
  - `DEFAULT 'a;b'`
  - `CREATE VIEW ... SELECT 'a;b'`

## Current Validation Result

This was not reproduced as a current black-box `br init` failure on the existing schema, but it was reproduced directly against the exact helper logic upstream currently uses for schema execution.

## Normal Use Trigger

This path is most likely to matter during:

- schema initialization
- migration execution
- any future schema change that introduces SQL string literals containing semicolons
- any maintenance work where multiple SQL statements are authored as a single batch and assumed to be parsed correctly by the backend

It is not primarily a day-to-day end-user command issue. It is a maintenance and migration safety issue.

## User-Visible Symptom

If a migration or schema change introduces this pattern, symptoms would likely appear as:

- broken init or migration behavior
- unexpected SQL parse failures
- partial schema application
- environment-specific failures where a valid SQL statement fails only because the batch executor split it incorrectly

## Affected Commands

- `br init`
- first-run repository setup on a clean checkout
- any future command path that invokes schema creation or migration

More indirectly affected surfaces:

- any command executed immediately after a failed or partially applied migration
- CI or installer flows that bootstrap a fresh repo and expect schema creation to be robust

## Severity / Operational Impact

This is medium severity maintenance debt. It is less urgent than the concurrency and blocked-cache issues, but it is still a concrete schema-execution bug class worth removing.

The risk is concentrated around future change velocity: it makes schema evolution less safe, increases the chance of subtle migration regressions, and leaves correctness dependent on a text-splitting workaround rather than SQL execution semantics.

## Suggested Fix Direction

- use real batch execution semantics
- add regressions that cover at least:
  - `INSERT ... VALUES('a;b')`
  - `DEFAULT 'a;b'`
  - `CREATE VIEW ... SELECT 'a;b'`
- if `fsqlite` cannot support this safely, isolate the limitation clearly or replace the execution path rather than relying on raw string splitting

## Acceptance Criteria

- schema execution no longer depends on manual semicolon splitting
- semicolons inside SQL string literals are handled correctly
- regression coverage exists for that case
```

## Suggested Cover Note

If you want to send a short DM or Discord note before opening issues, this is a clean version:

```md
I ran a focused validation pass against latest upstream `br` because I wanted to separate historical `fsqlite` concerns from current reproducible behavior.

I wrote up the findings here:
- maintainer handoff: `docs/UPSTREAM_FSQLITE_FINDINGS_2026-03-08.md`

The highest-signal current issues are:
- blocked-cache truncation beyond 50 parent-child levels
- failing concurrency tests, including WAL corruption
- upstream-created `.beads/beads.db` not being readable by standard SQLite despite `doctor` reporting integrity `ok`

I also prepared individual issue drafts so they can be opened cleanly if useful:
- `docs/UPSTREAM_FSQLITE_ISSUE_DRAFTS_2026-03-08.md`
```
