# `br` Storage Backend Technical Note: `fsqlite` to `rusqlite`

## Scope

This document is a fact-based summary of:

- the storage failures observed in current `beads_rust`
- the technical findings from direct source inspection
- the `rusqlite` port work completed in the fork branch `rusqlite-default-backend`
- the validation completed so far
- the remaining work required before treating the fork as fully production-ready

It is written to be shareable with upstream maintainers and reviewers. It
intentionally excludes local workflow notes, personal context, and speculation
about motives.

## Executive Summary

The current `beads_rust` runtime depends on `fsqlite` / `frankensqlite`, a
custom SQLite-style engine implemented in Rust. The fork replaces that runtime
backend with `rusqlite` (using bundled SQLite) while preserving:

- the `br` CLI surface
- the `.beads/` directory model
- the SQLite + JSONL architecture
- the existing schema and JSONL workflow

The goal of the fork is not to redesign `br`. The goal is to keep the product
model intact while restoring standard SQLite storage semantics and removing the
custom storage engine from the runtime path.

## Observed Problem Classes

The relevant observed failures fall into two categories:

### 1. Storage-Engine Correctness Failures

Examples:

- `cursor must be on a leaf to delete`
- `OpenRead failed: could not open storage cursor on root page ...`
- failures during write paths that rebuild `blocked_issues_cache`

The `cursor must be on a leaf to delete` message is a B-tree invariant failure.
In a SQLite-style engine, row deletes are expected to act on leaf pages. This
error indicates the engine attempted a delete while the cursor was positioned in
an invalid context for that operation. That points to a storage-engine bug or
internal tree-state corruption, not to a bead-graph logic error.

### 2. Application-Level Correctness Gaps

Examples:

- `MAX_DEPTH=50` truncating transitive blocked propagation
- `--no-db` prefix resolution becoming brittle when JSONL prefix inference is
  ambiguous
- `fsqlite`-driven SQL-shape workarounds that moved logic out of standard SQL

These are separate from engine corruption, but they affect command correctness
and can surface in the same operational paths.

## Key Technical Findings

### `fsqlite` Is Not Built On `rusqlite`

Direct source inspection shows:

- `fsqlite` / `frankensqlite` is its own SQLite-style engine
- it includes its own pager, WAL, B-tree, parser, planner, and related layers
- `rusqlite` appears in that workspace as a comparison/reference dependency, not
  as the runtime engine used by `br`

This means the migration is a real backend port, not a wrapper swap.

### `MAX_DEPTH=50` Was Not Required For Termination

The transitive blocked-cache propagation loop is monotonic:

- each pass only inserts descendants not already present in
  `blocked_issues_cache`
- once inserted, an issue is excluded from future passes
- the number of issues is finite

That means the loop already has a natural fixed point:

- it terminates when no new blocked descendants are found
- in the worst case it can only add each issue once

The hardcoded `50` cap was not required for termination. It caused silent
functional truncation for deep dependency graphs.

## Implemented Changes In The Fork

The following changes are already implemented in the fork branch
`rusqlite-default-backend`.

### Backend Port

- Added an adapter layer at `src/storage/db.rs`
- Re-routed storage code to use adapter-level `Connection`, `DbError`, `Row`,
  and `SqliteValue`
- Replaced the `fsqlite` crate family in `Cargo.toml` with:
  - `rusqlite = { version = "0.37", features = ["bundled"] }`
- Removed the `[patch.crates-io]` overrides that redirected `fsqlite-*` crates
  to `frankensqlite`
- Updated `src/error/mod.rs` so `BeadsError::Database` wraps the local adapter
  error rather than `FrankenError`

### Blocked-Cache Correctness

- Removed the hardcoded `MAX_DEPTH=50` truncation in
  `rebuild_blocked_cache_impl`
- Replaced it with a convergence-based bound tied to the total issue count
- Changed the behavior from "warn and return partial cache" to "return explicit
  error if the computed bound is ever exceeded"

### Write-Path and Schema Cleanup

- `dirty_issues` writes now use real SQLite upsert semantics keyed by `issue_id`
- `export_hashes` writes now use real SQLite upsert semantics keyed by
  `issue_id`
- `upsert_issue_for_import` now performs a true row upsert on `issues(id)`
  instead of `DELETE + INSERT`
- `config` and `metadata` are back to keyed tables (`key PRIMARY KEY`)
- `set_config`, `set_metadata`, and `set_metadata_in_tx` now use native SQLite
  upserts keyed by `key`
- `create_issue` no longer does a manual duplicate-ID probe; it relies on the
  `issues(id)` primary key atomically
- schema batch execution now uses real SQLite `execute_batch` instead of manual
  semicolon splitting
- `get_epic_counts` now uses grouped SQL aggregation instead of fetching rows
  and aggregating in Rust
- `init` now writes a real `issue_prefix` entry into `.beads/config.yaml`
  instead of leaving the prefix commented out

## Validation Completed

### Targeted Regression Coverage

The fork now includes targeted regression coverage for the highest-risk paths:

- deep parent-child blocked propagation beyond 50 levels
- repeated blocked-cache rebuild mutations with integrity checks
- file-backed two-writer contention with clean lock/busy behavior
- import upsert preserving related rows instead of deleting them
- native batch execution with SQL string literals containing semicolons

### Storage-Focused Test Slices

The following storage-focused suites passed after the port and follow-up fixes:

- `storage_blocked_cache`
- `storage_crud`
- `storage_deps`
- `storage_ready`

Additional focused validations passed for:

- `upsert_issue_for_import`
- `config` / `metadata` behavior
- duplicate-ID enforcement
- schema migration rebuilding legacy non-keyed `config` / `metadata`

### Critical CLI / E2E Slices

The following focused command-matrix slice passed:

- `e2e_basic_lifecycle`
- `e2e_ready`
- `e2e_epic`
- `e2e_sync_artifacts`
- `e2e_sync_preflight_integration`

This validates the most important command families for day-to-day use:

- create / update / dependency mutation
- ready / blocked evaluation
- epic status logic
- sync export/import preflight
- JSONL artifact handling

### Manual Canary Run

A manual disposable-workspace canary run using the built CLI binary completed
successfully for:

- `br init`
- `br create`
- `br dep add`
- `br ready --json`
- `br sync --status`
- `br --no-db list --json`

This provides a direct black-box confirmation that the fork behaves correctly in
normal CLI use outside the test harness.

## Remaining Work

The port is in a strong canary state, but there is still follow-up work before
calling it fully production-ready.

### 1. Broader Validation

- run a broader or full test suite pass on the standalone fork checkout
- continue real-world dogfooding with actual issue-tracking workloads
- keep validating `PRAGMA integrity_check` in representative file-backed runs

### 2. Remove Remaining `fsqlite` SQL-Shape Workarounds

The runtime engine has already changed, but some higher-level query logic still
reflects old `fsqlite` limitations. The highest-value remaining cleanup areas
are the places where SQL was deliberately reshaped to avoid old backend
limitations.

The main remaining category is:

- Rust-side traversal or filtering that exists only because `fsqlite` lacked
  support for the standard SQL form (for example, recursive parent filtering
  that still avoids `WITH RECURSIVE`)

These are no longer storage-corruption risks, but they can still create:

- unnecessary complexity
- performance drag
- avoidable edge-case behavior differences

### 3. Packaging / Distribution For The Fork

For local use, the fork can already be built and installed via:

- `cargo build --release`
- `cargo install --path . --force`

If the fork is intended for wider sharing, the remaining packaging work is:

- update `install.sh` defaults if the installer should target the fork's own
  release assets
- publish release artifacts from the fork if binary-install flows are desired
- update any Homebrew formula or tap strategy only after the release pipeline is
  defined

## Recommended Upstreamable Shape

The smallest reviewable upstream change is:

- keep the CLI contract
- keep the schema
- keep the JSONL format
- keep the `.beads/` model
- switch the default runtime backend to `rusqlite`

That keeps the product behavior stable while changing only the storage engine
underneath it.

If dual-backend support is ever considered, it should be treated as a separate
follow-up decision. The current fork work is intentionally focused on backend
stability first.

## Current Status

Current status is best described as:

- backend port implemented
- critical regressions covered
- focused command-matrix slice green
- manual canary green
- suitable for controlled real-world dogfooding
- not yet fully signed off as "done" until broader validation and remaining
  cleanup are completed

## Bottom Line

The fork demonstrates that `br` can preserve its current product model while
running on standard SQLite semantics through `rusqlite`.

The key result is not a redesign. The key result is that the same
SQLite-plus-JSONL architecture can be kept, while removing the custom storage
engine from the runtime path and restoring predictable database behavior.
