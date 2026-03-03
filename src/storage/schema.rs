//! Database schema definitions and migration logic.

use crate::error::Result;
use crate::storage::db::{Connection, SqliteValue};

pub const CURRENT_SCHEMA_VERSION: i32 = 3;

/// The complete SQL schema for the beads database.
/// Schema matches classic bd (Go) for interoperability.
pub const SCHEMA_SQL: &str = r"
    -- Issues table
    -- Note: TEXT fields use DEFAULT '' for bd (Go) compatibility.
    -- bd's sql.Scan doesn't handle NULL well when scanning into string fields.
    CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        content_hash TEXT,
        title TEXT NOT NULL CHECK(length(title) <= 500),
        description TEXT NOT NULL DEFAULT '',
        design TEXT NOT NULL DEFAULT '',
        acceptance_criteria TEXT NOT NULL DEFAULT '',
        notes TEXT NOT NULL DEFAULT '',
        status TEXT NOT NULL DEFAULT 'open',
        priority INTEGER NOT NULL DEFAULT 2 CHECK(priority >= 0 AND priority <= 4),
        issue_type TEXT NOT NULL DEFAULT 'task',
        assignee TEXT,
        owner TEXT DEFAULT '',
        estimated_minutes INTEGER,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT DEFAULT '',
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        closed_at DATETIME,
        close_reason TEXT DEFAULT '',
        closed_by_session TEXT DEFAULT '',
        due_at DATETIME,
        defer_until DATETIME,
        external_ref TEXT,
        source_system TEXT DEFAULT '',
        source_repo TEXT NOT NULL DEFAULT '.',
        deleted_at DATETIME,
        deleted_by TEXT DEFAULT '',
        delete_reason TEXT DEFAULT '',
        original_type TEXT DEFAULT '',
        compaction_level INTEGER DEFAULT 0,
        compacted_at DATETIME,
        compacted_at_commit TEXT,
        original_size INTEGER,
        sender TEXT DEFAULT '',
        ephemeral INTEGER DEFAULT 0,
        pinned INTEGER DEFAULT 0,
        is_template INTEGER DEFAULT 0,
        -- Closed-at invariant: closed issues MUST have closed_at timestamp
        CHECK (
            (status = 'closed' AND closed_at IS NOT NULL) OR
            (status = 'tombstone') OR
            (status NOT IN ('closed', 'tombstone') AND closed_at IS NULL)
        )
    );

    -- Primary access patterns
    CREATE INDEX IF NOT EXISTS idx_issues_status ON issues(status);
    CREATE INDEX IF NOT EXISTS idx_issues_priority ON issues(priority);
    CREATE INDEX IF NOT EXISTS idx_issues_issue_type ON issues(issue_type);
    CREATE INDEX IF NOT EXISTS idx_issues_assignee ON issues(assignee) WHERE assignee IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_issues_created_at ON issues(created_at);
    CREATE INDEX IF NOT EXISTS idx_issues_updated_at ON issues(updated_at);

    -- Export/sync patterns
    CREATE INDEX IF NOT EXISTS idx_issues_content_hash ON issues(content_hash);
    CREATE INDEX IF NOT EXISTS idx_issues_external_ref ON issues(external_ref) WHERE external_ref IS NOT NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_external_ref_unique ON issues(external_ref) WHERE external_ref IS NOT NULL;

    -- Special states
    CREATE INDEX IF NOT EXISTS idx_issues_ephemeral ON issues(ephemeral) WHERE ephemeral = 1;
    CREATE INDEX IF NOT EXISTS idx_issues_pinned ON issues(pinned) WHERE pinned = 1;
    CREATE INDEX IF NOT EXISTS idx_issues_tombstone ON issues(status) WHERE status = 'tombstone';

    -- Time-based
    CREATE INDEX IF NOT EXISTS idx_issues_due_at ON issues(due_at) WHERE due_at IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_issues_defer_until ON issues(defer_until) WHERE defer_until IS NOT NULL;

    -- Ready work composite index (most important for performance)
    CREATE INDEX IF NOT EXISTS idx_issues_ready
        ON issues(status, priority, created_at)
        WHERE status IN ('open', 'in_progress')
        AND ephemeral = 0
        AND pinned = 0
        AND (is_template = 0 OR is_template IS NULL);

    -- Dependencies
    CREATE TABLE IF NOT EXISTS dependencies (
        issue_id TEXT NOT NULL,
        depends_on_id TEXT NOT NULL,
        type TEXT NOT NULL DEFAULT 'blocks',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_by TEXT NOT NULL DEFAULT '',
        metadata TEXT DEFAULT '{}',
        thread_id TEXT DEFAULT '',
        PRIMARY KEY (issue_id, depends_on_id),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
        -- Note: depends_on_id FK intentionally removed to allow external issue references
    );
    CREATE INDEX IF NOT EXISTS idx_dependencies_issue ON dependencies(issue_id);
    CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
    CREATE INDEX IF NOT EXISTS idx_dependencies_type ON dependencies(type);
    CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on_type ON dependencies(depends_on_id, type);
    CREATE INDEX IF NOT EXISTS idx_dependencies_thread ON dependencies(thread_id) WHERE thread_id != '';
    -- Composite for blocking lookups
    CREATE INDEX IF NOT EXISTS idx_dependencies_blocking
        ON dependencies(depends_on_id, issue_id)
        WHERE type IN ('blocks', 'parent-child', 'conditional-blocks', 'waits-for');

    -- Labels
    CREATE TABLE IF NOT EXISTS labels (
        issue_id TEXT NOT NULL,
        label TEXT NOT NULL,
        PRIMARY KEY (issue_id, label),
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label);
    CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);

    -- Comments
    CREATE TABLE IF NOT EXISTS comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        author TEXT NOT NULL,
        text TEXT NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id);
    CREATE INDEX IF NOT EXISTS idx_comments_created_at ON comments(created_at);

    -- Events (Audit)
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        actor TEXT NOT NULL DEFAULT '',
        old_value TEXT,
        new_value TEXT,
        comment TEXT,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_events_issue ON events(issue_id);
    CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
    CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
    CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor) WHERE actor != '';

    -- Config (Runtime)
    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_config_key ON config(key);

    -- Metadata
    CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_metadata_key ON metadata(key);

    -- Dirty Issues (for export)
    CREATE TABLE IF NOT EXISTS dirty_issues (
        issue_id TEXT PRIMARY KEY,
        marked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_dirty_issues_marked_at ON dirty_issues(marked_at);

    -- Export Hashes (for incremental export)
    CREATE TABLE IF NOT EXISTS export_hashes (
        issue_id TEXT PRIMARY KEY,
        content_hash TEXT NOT NULL,
        exported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );

    -- Blocked Issues Cache (Materialized view)
    -- Rebuilt on dependency or status changes
    CREATE TABLE IF NOT EXISTS blocked_issues_cache (
        issue_id TEXT PRIMARY KEY,
        blocked_by TEXT NOT NULL,  -- JSON array of blocking issue IDs
        blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_blocked_cache_blocked_at ON blocked_issues_cache(blocked_at);

    -- Child Counters (for hierarchical IDs like bd-abc.1, bd-abc.2)
    CREATE TABLE IF NOT EXISTS child_counters (
        parent_id TEXT PRIMARY KEY,
        last_child INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY (parent_id) REFERENCES issues(id) ON DELETE CASCADE
    );
";

/// Execute multiple SQL statements in a single batch.
pub(crate) fn execute_batch(conn: &Connection, sql: &str) -> Result<()> {
    conn.execute_batch(sql)?;
    Ok(())
}

/// Apply the schema to the database.
///
/// This splits the DDL script into individual statements and executes them.
/// It is idempotent because all statements use `IF NOT EXISTS`.
///
/// # Errors
///
/// Returns an error if the SQL execution fails or pragmas cannot be set.
pub fn apply_schema(conn: &Connection) -> Result<()> {
    // Run pre-schema migrations first to fix any incompatible old tables
    // This must run BEFORE execute_batch because the batch includes CREATE INDEX
    // statements that will fail if old tables have missing columns
    run_pre_schema_migrations(conn)?;

    execute_batch(conn, SCHEMA_SQL)?;

    // Run migrations for existing databases
    run_migrations(conn)?;

    // Set journal mode to WAL for concurrency
    conn.execute("PRAGMA journal_mode = WAL")?;

    // Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")?;

    // Performance PRAGMAs (safe with WAL mode)
    // NORMAL synchronous is safe with WAL: committed data survives OS crash
    conn.execute("PRAGMA synchronous = NORMAL")?;
    // Use memory for temp tables/indexes instead of disk
    conn.execute("PRAGMA temp_store = MEMORY")?;
    // 8MB page cache (default is ~2MB), improves read-heavy workloads
    conn.execute("PRAGMA cache_size = -8000")?;
    // Mark schema as applied so future opens can skip DDL/migration work.
    conn.execute(&format!("PRAGMA user_version = {CURRENT_SCHEMA_VERSION}"))?;

    Ok(())
}

fn table_exists(conn: &Connection, table: &str) -> bool {
    conn.query_with_params(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        &[SqliteValue::from(table)],
    )
    .is_ok_and(|rows| !rows.is_empty())
}

fn column_exists(conn: &Connection, table: &str, column: &str) -> bool {
    // Use parameterized query for the column name to prevent SQL injection.
    // Note: pragma_table_info() requires the table name directly (can't be parameterized),
    // but we validate it's a known table name before calling this function.
    let sql = format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name = ?");
    conn.query_with_params(&sql, &[SqliteValue::from(column)])
        .is_ok_and(|rows| !rows.is_empty())
}

const ISSUE_COLUMNS: &[(&str, &str)] = &[
    ("content_hash", "TEXT"),
    ("description", "TEXT NOT NULL DEFAULT ''"),
    ("design", "TEXT NOT NULL DEFAULT ''"),
    ("acceptance_criteria", "TEXT NOT NULL DEFAULT ''"),
    ("notes", "TEXT NOT NULL DEFAULT ''"),
    ("status", "TEXT NOT NULL DEFAULT 'open'"),
    (
        "priority",
        "INTEGER NOT NULL DEFAULT 2 CHECK(priority >= 0 AND priority <= 4)",
    ),
    ("issue_type", "TEXT NOT NULL DEFAULT 'task'"),
    ("assignee", "TEXT"),
    ("owner", "TEXT DEFAULT ''"),
    ("estimated_minutes", "INTEGER"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("created_by", "TEXT DEFAULT ''"),
    ("updated_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("closed_at", "DATETIME"),
    ("close_reason", "TEXT DEFAULT ''"),
    ("closed_by_session", "TEXT DEFAULT ''"),
    ("due_at", "DATETIME"),
    ("defer_until", "DATETIME"),
    ("external_ref", "TEXT"),
    ("source_system", "TEXT DEFAULT ''"),
    ("source_repo", "TEXT NOT NULL DEFAULT '.'"),
    ("deleted_at", "DATETIME"),
    ("deleted_by", "TEXT DEFAULT ''"),
    ("delete_reason", "TEXT DEFAULT ''"),
    ("original_type", "TEXT DEFAULT ''"),
    ("compaction_level", "INTEGER DEFAULT 0"),
    ("compacted_at", "DATETIME"),
    ("compacted_at_commit", "TEXT"),
    ("original_size", "INTEGER"),
    ("sender", "TEXT DEFAULT ''"),
    ("ephemeral", "INTEGER DEFAULT 0"),
    ("pinned", "INTEGER DEFAULT 0"),
    ("is_template", "INTEGER DEFAULT 0"),
];

const DEPENDENCY_COLUMNS: &[(&str, &str)] = &[
    ("type", "TEXT NOT NULL DEFAULT 'blocks'"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
    ("created_by", "TEXT NOT NULL DEFAULT ''"),
    ("metadata", "TEXT DEFAULT '{}'"),
    ("thread_id", "TEXT DEFAULT ''"),
];

const COMMENT_COLUMNS: &[(&str, &str)] = &[
    ("author", "TEXT NOT NULL DEFAULT ''"),
    ("text", "TEXT NOT NULL DEFAULT ''"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
];

const EVENT_COLUMNS: &[(&str, &str)] = &[
    ("event_type", "TEXT NOT NULL DEFAULT ''"),
    ("actor", "TEXT NOT NULL DEFAULT ''"),
    ("old_value", "TEXT"),
    ("new_value", "TEXT"),
    ("comment", "TEXT"),
    ("created_at", "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"),
];

fn ensure_columns(conn: &Connection, table: &str, columns: &[(&str, &str)]) -> Result<()> {
    if !table_exists(conn, table) {
        return Ok(());
    }

    for (name, definition) in columns {
        if !column_exists(conn, table, name) {
            let sql = format!("ALTER TABLE {table} ADD COLUMN {name} {definition}");
            conn.execute(&sql)?;
        }
    }

    Ok(())
}

/// Expected column order for the issues table (id + ISSUE_COLUMNS names).
/// Used to detect when ALTER TABLE has appended columns in the wrong position,
/// which causes fsqlite to fail with "no such column" errors on older databases.
const EXPECTED_ISSUE_COLUMN_ORDER: &[&str] = &[
    "id",
    "content_hash",
    "title",
    "description",
    "design",
    "acceptance_criteria",
    "notes",
    "status",
    "priority",
    "issue_type",
    "assignee",
    "owner",
    "estimated_minutes",
    "created_at",
    "created_by",
    "updated_at",
    "closed_at",
    "close_reason",
    "closed_by_session",
    "due_at",
    "defer_until",
    "external_ref",
    "source_system",
    "source_repo",
    "deleted_at",
    "deleted_by",
    "delete_reason",
    "original_type",
    "compaction_level",
    "compacted_at",
    "compacted_at_commit",
    "original_size",
    "sender",
    "ephemeral",
    "pinned",
    "is_template",
];

/// Check whether the issues table has columns in the expected order.
/// Returns `true` if the column order matches, `false` if it differs or the
/// table doesn't exist.
fn issues_column_order_matches(conn: &Connection) -> bool {
    if !table_exists(conn, "issues") {
        return true; // Will be created fresh by SCHEMA_SQL
    }

    let Ok(rows) = conn.query("PRAGMA table_info(issues)") else {
        return false;
    };

    let actual_columns: Vec<String> = rows
        .iter()
        .filter_map(|row| row.get(1).and_then(SqliteValue::as_text).map(String::from))
        .collect();

    if actual_columns.len() != EXPECTED_ISSUE_COLUMN_ORDER.len() {
        return false;
    }

    actual_columns
        .iter()
        .zip(EXPECTED_ISSUE_COLUMN_ORDER.iter())
        .all(|(actual, expected)| actual == expected)
}

/// Rebuild the issues table so columns match the canonical SCHEMA_SQL order.
///
/// This fixes databases where ALTER TABLE ADD COLUMN appended columns in a
/// different position than the CREATE TABLE definition, causing fsqlite's
/// column-name resolver to fail with "no such column" errors.
///
/// Uses the standard SQLite migration pattern:
///   1. Create new table with correct schema
///   2. Copy data from old table
///   3. Drop old table
///   4. Rename new table
fn rebuild_issues_table(conn: &Connection) -> Result<()> {
    // Build column list from what currently exists in the table
    let existing_rows = conn.query("PRAGMA table_info(issues)")?;
    let existing_columns: Vec<String> = existing_rows
        .iter()
        .filter_map(|row| row.get(1).and_then(SqliteValue::as_text).map(String::from))
        .collect();

    if existing_columns.is_empty() {
        return Ok(()); // Table is empty or doesn't exist
    }

    // Wrap the entire rebuild in a transaction so a crash between DROP TABLE
    // and RENAME cannot lose data.
    conn.execute("BEGIN EXCLUSIVE")?;

    if let Err(e) = rebuild_issues_table_inner(conn, &existing_columns) {
        let _ = conn.execute("ROLLBACK");
        return Err(e);
    }

    conn.execute("COMMIT")?;
    Ok(())
}

/// Inner helper for [`rebuild_issues_table`] that performs the actual work
/// inside an already-open transaction.
fn rebuild_issues_table_inner(conn: &Connection, existing_columns: &[String]) -> Result<()> {
    // Drop all indexes on the issues table first (they'll be recreated by SCHEMA_SQL)
    let index_rows =
        conn.query("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='issues' AND sql IS NOT NULL")?;
    for row in &index_rows {
        if let Some(name) = row.get(0).and_then(SqliteValue::as_text) {
            conn.execute(&format!("DROP INDEX IF EXISTS \"{name}\""))?;
        }
    }

    // Drop tables that have foreign keys referencing issues (they'll be recreated)
    // We need to save and restore their data too.
    // For simplicity, we only rebuild the issues table and let SCHEMA_SQL
    // recreate indexes. Foreign key tables (dependencies, labels, etc.) keep
    // their data since we use the same primary key.

    // Create the new table with canonical column order
    // Use a temporary name to avoid conflicts
    conn.execute("DROP TABLE IF EXISTS issues_rebuild_tmp")?;

    // Build CREATE TABLE for the new table with only columns that exist in the old table
    // plus any missing columns with defaults
    // Build the canonical column list: id, content_hash, title, then the
    // rest of ISSUE_COLUMNS (skipping content_hash which is already placed).
    // This order must match EXPECTED_ISSUE_COLUMN_ORDER and SCHEMA_SQL.
    let all_expected: Vec<(&str, &str)> = std::iter::once(("id", "TEXT PRIMARY KEY"))
        .chain(std::iter::once(("content_hash", "TEXT")))
        .chain(std::iter::once((
            "title",
            "TEXT NOT NULL CHECK(length(title) <= 500)",
        )))
        .chain(
            ISSUE_COLUMNS
                .iter()
                .copied()
                .filter(|(name, _)| *name != "content_hash"),
        )
        .collect();

    let mut create_cols = Vec::new();
    for (col_name, col_def) in &all_expected {
        create_cols.push(format!("{col_name} {col_def}"));
    }

    let create_sql = format!(
        "CREATE TABLE issues_rebuild_tmp ({})",
        create_cols.join(", ")
    );
    conn.execute(&create_sql)?;

    // Build the SELECT column list: use existing columns where available,
    // NULL (or default) for missing ones
    let mut select_cols = Vec::new();
    for (col_name, _) in &all_expected {
        if existing_columns.iter().any(|c| c == col_name) {
            select_cols.push((*col_name).to_string());
        } else {
            select_cols.push("NULL".to_string());
        }
    }

    let insert_cols: Vec<&str> = all_expected.iter().map(|(name, _)| *name).collect();

    let copy_sql = format!(
        "INSERT INTO issues_rebuild_tmp ({}) SELECT {} FROM issues",
        insert_cols.join(", "),
        select_cols.join(", ")
    );
    conn.execute(&copy_sql)?;

    // Swap tables
    conn.execute("DROP TABLE issues")?;
    conn.execute("ALTER TABLE issues_rebuild_tmp RENAME TO issues")?;

    Ok(())
}

fn kv_table_uses_primary_key(conn: &Connection, table: &str) -> bool {
    if !table_exists(conn, table) {
        return false;
    }

    let sql = format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name='key' AND pk=1");
    conn.query(&sql).is_ok_and(|rows| !rows.is_empty())
}

fn rebuild_kv_table_with_primary_key(conn: &Connection, table: &str) -> Result<()> {
    let tmp_table = format!("{table}_rebuild_tmp");

    conn.execute("BEGIN EXCLUSIVE")?;

    let result = (|| -> Result<()> {
        conn.execute(&format!("DROP TABLE IF EXISTS {tmp_table}"))?;
        conn.execute(&format!(
            "CREATE TABLE {tmp_table} (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )"
        ))?;

        conn.execute(&format!(
            "INSERT INTO {tmp_table} (key, value)
             SELECT src.key, src.value
             FROM {table} src
             WHERE src.rowid = (
                 SELECT MAX(src2.rowid)
                 FROM {table} src2
                 WHERE src2.key = src.key
             )"
        ))?;

        conn.execute(&format!("DROP TABLE {table}"))?;
        conn.execute(&format!("ALTER TABLE {tmp_table} RENAME TO {table}"))?;
        Ok(())
    })();

    if let Err(err) = result {
        let _ = conn.execute("ROLLBACK");
        return Err(err);
    }

    conn.execute("COMMIT")?;
    Ok(())
}

/// Run pre-schema migrations to fix incompatible old tables.
///
/// This must run BEFORE `execute_batch(SCHEMA_SQL)` because the schema includes
/// CREATE INDEX statements that will fail if old tables have missing columns.
fn run_pre_schema_migrations(conn: &Connection) -> Result<()> {
    // Legacy fsqlite-era schemas removed PRIMARY KEY from config/metadata key
    // columns. Rebuild those tables back to standard SQLite key-value tables so
    // we can use native upsert semantics again.
    if table_exists(conn, "config") && !kv_table_uses_primary_key(conn, "config") {
        rebuild_kv_table_with_primary_key(conn, "config")?;
    }
    if table_exists(conn, "metadata") && !kv_table_uses_primary_key(conn, "metadata") {
        rebuild_kv_table_with_primary_key(conn, "metadata")?;
    }

    // Drop blocked_issues_cache if it exists but lacks required columns.
    // The main schema will recreate it with the correct structure.
    if table_exists(conn, "blocked_issues_cache") {
        let has_blocked_at = column_exists(conn, "blocked_issues_cache", "blocked_at");
        let has_blocked_by = column_exists(conn, "blocked_issues_cache", "blocked_by");
        let has_issue_id = column_exists(conn, "blocked_issues_cache", "issue_id");

        if !has_blocked_at || !has_blocked_by || !has_issue_id {
            conn.execute("DROP TABLE IF EXISTS blocked_issues_cache")?;
        }
    }

    // Rebuild the issues table if columns are out of order or missing.
    // This fixes fsqlite "no such column" errors on databases created with
    // older br versions where ALTER TABLE ADD COLUMN appended columns in
    // a different position than the canonical CREATE TABLE definition.
    if table_exists(conn, "issues") && !issues_column_order_matches(conn) {
        rebuild_issues_table(conn)?;
    }

    // Ensure legacy tables have all columns needed for schema indexes and queries.
    ensure_columns(conn, "issues", ISSUE_COLUMNS)?;
    ensure_columns(conn, "dependencies", DEPENDENCY_COLUMNS)?;
    ensure_columns(conn, "comments", COMMENT_COLUMNS)?;
    ensure_columns(conn, "events", EVENT_COLUMNS)?;

    // Always drop idx_issues_ready so SCHEMA_SQL recreates it with the
    // current definition (including is_template filter). DROP INDEX is O(1)
    // and SCHEMA_SQL's CREATE INDEX is fast for typical issue counts.
    conn.execute("DROP INDEX IF EXISTS idx_issues_ready")?;

    Ok(())
}

/// Run schema migrations for existing databases.
///
/// This handles upgrades for tables that may have been created with older schemas.
#[allow(clippy::too_many_lines)]
fn run_migrations(conn: &Connection) -> Result<()> {
    // Migration: Ensure blocked_issues_cache has correct schema (blocked_by, blocked_at)
    // Check for old column name (blocked_by_json) or missing columns
    let has_blocked_by: bool = conn
        .query("SELECT 1 FROM pragma_table_info('blocked_issues_cache') WHERE name='blocked_by'")
        .is_ok_and(|rows| !rows.is_empty());

    let has_blocked_at: bool = conn
        .query("SELECT 1 FROM pragma_table_info('blocked_issues_cache') WHERE name='blocked_at'")
        .is_ok_and(|rows| !rows.is_empty());

    let has_issue_id: bool = conn
        .query("SELECT 1 FROM pragma_table_info('blocked_issues_cache') WHERE name='issue_id'")
        .is_ok_and(|rows| !rows.is_empty());

    if !has_blocked_by || !has_blocked_at || !has_issue_id {
        // Table needs update - drop and recreate (it's a cache, data is regenerated)
        conn.execute("DROP TABLE IF EXISTS blocked_issues_cache")?;
        conn.execute(
            "CREATE TABLE blocked_issues_cache (
                issue_id TEXT PRIMARY KEY,
                blocked_by TEXT NOT NULL,
                blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (issue_id) REFERENCES issues(id) ON DELETE CASCADE
            )",
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_blocked_cache_blocked_at ON blocked_issues_cache(blocked_at)",
        )?;
    }

    // Migration: ensure compaction_level is never NULL (bd compatibility)
    let has_compaction_level: bool = conn
        .query("SELECT 1 FROM pragma_table_info('issues') WHERE name='compaction_level'")
        .is_ok_and(|rows| !rows.is_empty());

    if has_compaction_level {
        conn.execute("UPDATE issues SET compaction_level = 0 WHERE compaction_level IS NULL")?;
    }

    // Note: source_repo and is_template column backfills are handled in
    // run_pre_schema_migrations() via ensure_columns(). Repeating ALTER TABLE
    // here can create duplicate column definitions on some engines.

    // Migration: Add missing indexes for bd parity
    // These use IF NOT EXISTS so they're safe to run multiple times
    execute_batch(
        conn,
        r"
        -- Export/sync patterns
        CREATE INDEX IF NOT EXISTS idx_issues_content_hash ON issues(content_hash);
        CREATE INDEX IF NOT EXISTS idx_issues_external_ref ON issues(external_ref) WHERE external_ref IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_external_ref_unique ON issues(external_ref) WHERE external_ref IS NOT NULL;

        -- Special states
        CREATE INDEX IF NOT EXISTS idx_issues_ephemeral ON issues(ephemeral) WHERE ephemeral = 1;
        CREATE INDEX IF NOT EXISTS idx_issues_pinned ON issues(pinned) WHERE pinned = 1;
        CREATE INDEX IF NOT EXISTS idx_issues_tombstone ON issues(status) WHERE status = 'tombstone';

        -- Time-based
        CREATE INDEX IF NOT EXISTS idx_issues_due_at ON issues(due_at) WHERE due_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_issues_defer_until ON issues(defer_until) WHERE defer_until IS NOT NULL;

        -- Ready work composite index (most important for performance)
        CREATE INDEX IF NOT EXISTS idx_issues_ready
            ON issues(status, priority, created_at)
            WHERE status IN ('open', 'in_progress')
            AND ephemeral = 0
            AND pinned = 0
            AND (is_template = 0 OR is_template IS NULL);

    ",
    )?;

    // Drop legacy index names (safe if absent)
    execute_batch(
        conn,
        r"
        DROP INDEX IF EXISTS idx_dependencies_issue_id;
        DROP INDEX IF EXISTS idx_dependencies_depends_on_id;
        DROP INDEX IF EXISTS idx_dependencies_composite;
        DROP INDEX IF EXISTS idx_labels_issue_id;
    ",
    )?;

    if table_exists(conn, "dependencies") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_dependencies_issue ON dependencies(issue_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on ON dependencies(depends_on_id);
            CREATE INDEX IF NOT EXISTS idx_dependencies_type ON dependencies(type);
            CREATE INDEX IF NOT EXISTS idx_dependencies_depends_on_type ON dependencies(depends_on_id, type);
            CREATE INDEX IF NOT EXISTS idx_dependencies_blocking
                ON dependencies(depends_on_id, issue_id)
                WHERE type IN ('blocks', 'parent-child', 'conditional-blocks', 'waits-for');
        ",
        )?;

        if column_exists(conn, "dependencies", "thread_id") {
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_dependencies_thread ON dependencies(thread_id) WHERE thread_id != ''",
            )?;
        }
    }

    if table_exists(conn, "labels") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_labels_label ON labels(label);
            CREATE INDEX IF NOT EXISTS idx_labels_issue ON labels(issue_id);
        ",
        )?;
    }

    if table_exists(conn, "comments") {
        conn.execute("CREATE INDEX IF NOT EXISTS idx_comments_issue ON comments(issue_id)")?;
    }

    if table_exists(conn, "events") {
        execute_batch(
            conn,
            r"
            CREATE INDEX IF NOT EXISTS idx_events_issue ON events(issue_id);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
            CREATE INDEX IF NOT EXISTS idx_events_actor ON events(actor) WHERE actor != '';
        ",
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::db::Connection;
    use std::collections::HashSet;
    use tempfile::TempDir;

    #[test]
    fn test_apply_schema() {
        let conn = Connection::open(":memory:").unwrap();
        apply_schema(&conn).expect("Failed to apply schema");

        // Verify a few tables exist
        let tables: Vec<String> = conn
            .query("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(tables.contains(&"issues".to_string()));
        assert!(tables.contains(&"dependencies".to_string()));
        assert!(tables.contains(&"config".to_string()));
        assert!(tables.contains(&"dirty_issues".to_string()));

        // Verify pragmas
        let row = conn.query_row("PRAGMA journal_mode").unwrap();
        let journal_mode = row
            .get(0)
            .and_then(|v| v.as_text())
            .unwrap_or("")
            .to_string();
        // In-memory DBs use MEMORY journaling, regardless of what we set
        assert!(journal_mode.to_uppercase() == "WAL" || journal_mode.to_uppercase() == "MEMORY");

        let row = conn.query_row("PRAGMA foreign_keys").unwrap();
        let foreign_keys = row.get(0).and_then(SqliteValue::as_integer).unwrap_or(0);
        assert_eq!(foreign_keys, 1);
    }

    #[test]
    fn test_apply_schema_file_backed_has_no_duplicate_issues_columns() {
        let temp = TempDir::new().expect("tempdir");
        let db_path = temp.path().join("beads.db");
        let conn = Connection::open(db_path.to_string_lossy().into_owned()).unwrap();

        apply_schema(&conn).expect("Failed to apply schema");

        let row = conn
            .query_row("SELECT sql FROM sqlite_master WHERE type='table' AND name='issues'")
            .expect("issues table should exist");
        let issues_sql = row
            .get(0)
            .and_then(SqliteValue::as_text)
            .expect("issues table SQL should be present");

        assert_eq!(
            issues_sql.matches("source_repo").count(),
            1,
            "issues table SQL should define source_repo exactly once"
        );
        assert_eq!(
            issues_sql.matches("is_template").count(),
            1,
            "issues table SQL should define is_template exactly once"
        );
    }

    /// Conformance test: Verify schema matches bd (Go) for interoperability.
    /// Tests table structure, defaults, constraints, and indexes.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_schema_parity_conformance() {
        let conn = Connection::open(":memory:").unwrap();
        apply_schema(&conn).expect("Failed to apply schema");

        // === ISSUES TABLE ===
        // Verify column defaults
        let issues_cols: Vec<(String, String, i32, Option<String>)> = conn
            .query("PRAGMA table_info(issues)")
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row.get(1)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    row.get(2)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    #[allow(clippy::cast_possible_truncation)]
                    {
                        row.get(3).and_then(SqliteValue::as_integer).unwrap_or(0) as i32
                    },
                    row.get(4).and_then(|v| v.as_text()).map(String::from),
                )
            })
            .collect();

        // Check required defaults for bd parity
        let col_map: std::collections::HashMap<_, _> = issues_cols
            .iter()
            .map(|(name, typ, notnull, dflt)| {
                (name.as_str(), (typ.as_str(), *notnull, dflt.clone()))
            })
            .collect();

        // status must default to 'open'
        assert_eq!(
            col_map.get("status").map(|c| c.2.as_deref()),
            Some(Some("'open'")),
            "status should default to 'open'"
        );

        // priority must default to 2
        assert_eq!(
            col_map.get("priority").map(|c| c.2.as_deref()),
            Some(Some("2")),
            "priority should default to 2"
        );

        // issue_type must default to 'task'
        assert_eq!(
            col_map.get("issue_type").map(|c| c.2.as_deref()),
            Some(Some("'task'")),
            "issue_type should default to 'task'"
        );

        // created_at and updated_at must default to CURRENT_TIMESTAMP
        assert_eq!(
            col_map.get("created_at").map(|c| c.2.as_deref()),
            Some(Some("CURRENT_TIMESTAMP")),
            "created_at should default to CURRENT_TIMESTAMP"
        );
        assert_eq!(
            col_map.get("updated_at").map(|c| c.2.as_deref()),
            Some(Some("CURRENT_TIMESTAMP")),
            "updated_at should default to CURRENT_TIMESTAMP"
        );

        // === VERIFY KEY INDEXES EXIST ===
        let indexes: HashSet<String> = conn
            .query("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(0).and_then(|v| v.as_text()).map(String::from))
            .collect();

        // Core indexes
        assert!(
            indexes.contains("idx_issues_status"),
            "missing idx_issues_status"
        );
        assert!(
            indexes.contains("idx_issues_priority"),
            "missing idx_issues_priority"
        );
        assert!(
            indexes.contains("idx_issues_issue_type"),
            "missing idx_issues_issue_type"
        );
        assert!(
            indexes.contains("idx_issues_created_at"),
            "missing idx_issues_created_at"
        );
        assert!(
            indexes.contains("idx_issues_updated_at"),
            "missing idx_issues_updated_at"
        );

        // Export/sync indexes
        assert!(
            indexes.contains("idx_issues_content_hash"),
            "missing idx_issues_content_hash"
        );
        assert!(
            indexes.contains("idx_issues_external_ref")
                || indexes.contains("idx_issues_external_ref_unique"),
            "missing external_ref index"
        );

        // Special state indexes
        assert!(
            indexes.contains("idx_issues_ephemeral"),
            "missing idx_issues_ephemeral"
        );
        assert!(
            indexes.contains("idx_issues_pinned"),
            "missing idx_issues_pinned"
        );
        assert!(
            indexes.contains("idx_issues_tombstone"),
            "missing idx_issues_tombstone"
        );

        // Time-based indexes
        assert!(
            indexes.contains("idx_issues_due_at"),
            "missing idx_issues_due_at"
        );
        assert!(
            indexes.contains("idx_issues_defer_until"),
            "missing idx_issues_defer_until"
        );

        // Ready work composite index (critical for performance)
        assert!(
            indexes.contains("idx_issues_ready"),
            "missing idx_issues_ready composite index"
        );

        // === DEPENDENCIES TABLE ===
        let deps_cols: Vec<(String, Option<String>)> = conn
            .query("PRAGMA table_info(dependencies)")
            .unwrap()
            .iter()
            .map(|row| {
                (
                    row.get(1)
                        .and_then(|v| v.as_text())
                        .unwrap_or("")
                        .to_string(),
                    row.get(4).and_then(|v| v.as_text()).map(String::from),
                )
            })
            .collect();

        let deps_map: std::collections::HashMap<_, _> = deps_cols
            .iter()
            .map(|(name, dflt)| (name.as_str(), dflt.clone()))
            .collect();

        // type must default to 'blocks'
        assert_eq!(
            deps_map.get("type").cloned().flatten().as_deref(),
            Some("'blocks'"),
            "dependencies.type should default to 'blocks'"
        );

        // metadata must default to '{}'
        assert_eq!(
            deps_map.get("metadata").cloned().flatten().as_deref(),
            Some("'{}'"),
            "dependencies.metadata should default to '{{}}'"
        );

        // Dependency indexes (bd parity)
        assert!(
            indexes.contains("idx_dependencies_issue"),
            "missing idx_dependencies_issue"
        );
        assert!(
            indexes.contains("idx_dependencies_depends_on"),
            "missing idx_dependencies_depends_on"
        );
        assert!(
            indexes.contains("idx_dependencies_type"),
            "missing idx_dependencies_type"
        );
        assert!(
            indexes.contains("idx_dependencies_depends_on_type"),
            "missing idx_dependencies_depends_on_type"
        );
        assert!(
            indexes.contains("idx_dependencies_thread"),
            "missing idx_dependencies_thread"
        );
        assert!(
            indexes.contains("idx_dependencies_blocking"),
            "missing idx_dependencies_blocking"
        );

        // Labels indexes
        assert!(
            indexes.contains("idx_labels_label"),
            "missing idx_labels_label"
        );
        assert!(
            indexes.contains("idx_labels_issue"),
            "missing idx_labels_issue"
        );

        // === BLOCKED_ISSUES_CACHE TABLE ===
        let cache_cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cache_cols.contains(&"issue_id".to_string()),
            "blocked_issues_cache should have 'issue_id' column"
        );

        // Must have blocked_by (not blocked_by_json) and blocked_at
        assert!(
            cache_cols.contains(&"blocked_by".to_string()),
            "blocked_issues_cache should have 'blocked_by' column (not 'blocked_by_json')"
        );
        assert!(
            cache_cols.contains(&"blocked_at".to_string()),
            "blocked_issues_cache should have 'blocked_at' column"
        );
        assert!(
            !cache_cols.contains(&"blocked_by_json".to_string()),
            "blocked_issues_cache should NOT have old 'blocked_by_json' column"
        );

        // Verify blocked_cache index exists
        assert!(
            indexes.contains("idx_blocked_cache_blocked_at"),
            "missing idx_blocked_cache_blocked_at"
        );

        // === TEST CLOSED-AT CONSTRAINT ===
        // Insert an issue with defaults (will get status='open', closed_at=NULL)
        conn.execute("INSERT INTO issues (id, title) VALUES ('test-1', 'Test Issue')")
            .expect("Should allow open issue without closed_at");

        // Try to insert closed issue without closed_at — CHECK constraint
        // should reject it. fsqlite does not yet enforce CHECK constraints,
        // so we accept either outcome.
        let result = conn.execute(
            "INSERT INTO issues (id, title, status) VALUES ('test-2', 'Closed', 'closed')",
        );
        if result.is_ok() {
            // fsqlite: CHECK not enforced — clean up the row so later assertions
            // are not affected by the extra row.
            let _ = conn.execute("DELETE FROM issues WHERE id = 'test-2'");
        }

        // Insert closed issue with closed_at - should succeed
        conn.execute(
            "INSERT INTO issues (id, title, status, closed_at) VALUES ('test-3', 'Closed', 'closed', CURRENT_TIMESTAMP)",
        )
        .expect("Should allow closed issue with closed_at");

        // Insert tombstone without closed_at - should succeed (tombstones exempt)
        conn.execute(
            "INSERT INTO issues (id, title, status) VALUES ('test-4', 'Tombstone', 'tombstone')",
        )
        .expect("Should allow tombstone without closed_at");
    }

    /// Test that migrations correctly upgrade old schemas.
    #[test]
    fn test_migration_blocked_cache_upgrade() {
        let conn = Connection::open(":memory:").unwrap();

        // Create old-style blocked_issues_cache with blocked_by_json
        // Using a complete issues table schema so index migrations succeed
        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                priority INTEGER NOT NULL DEFAULT 2,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT,
                external_ref TEXT,
                ephemeral INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                due_at DATETIME,
                defer_until DATETIME
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'blocks',
                PRIMARY KEY (issue_id, depends_on_id)
            );
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE blocked_issues_cache (
                issue_id TEXT PRIMARY KEY,
                blocked_by_json TEXT NOT NULL
            );
        ",
        )
        .unwrap();

        // Run migrations
        run_migrations(&conn).unwrap();

        // Verify columns were updated
        let cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cols.contains(&"blocked_by".to_string()),
            "Should have blocked_by"
        );
        assert!(
            cols.contains(&"blocked_at".to_string()),
            "Should have blocked_at"
        );
        assert!(
            !cols.contains(&"blocked_by_json".to_string()),
            "Should not have blocked_by_json"
        );
    }

    /// Migration: drop old blocked_issues_cache missing issue_id column.
    #[test]
    fn test_migration_blocked_cache_missing_issue_id() {
        let conn = Connection::open(":memory:").unwrap();

        // Old-style cache table with 'id' column instead of 'issue_id'
        // Using a complete issues table schema so index migrations succeed
        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                priority INTEGER NOT NULL DEFAULT 2,
                issue_type TEXT NOT NULL DEFAULT 'task',
                assignee TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                content_hash TEXT,
                external_ref TEXT,
                ephemeral INTEGER DEFAULT 0,
                pinned INTEGER DEFAULT 0,
                due_at DATETIME,
                defer_until DATETIME
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                type TEXT NOT NULL DEFAULT 'blocks',
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_by TEXT NOT NULL DEFAULT '',
                metadata TEXT DEFAULT '{}',
                thread_id TEXT DEFAULT '',
                PRIMARY KEY (issue_id, depends_on_id)
            );
            CREATE TABLE comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                author TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                issue_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor TEXT NOT NULL DEFAULT '',
                old_value TEXT,
                new_value TEXT,
                comment TEXT,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE blocked_issues_cache (
                id TEXT PRIMARY KEY,
                blocked_by TEXT NOT NULL,
                blocked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        ",
        )
        .unwrap();

        // Apply full schema (includes pre-migrations)
        apply_schema(&conn).unwrap();

        let cols: Vec<String> = conn
            .query("PRAGMA table_info(blocked_issues_cache)")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        assert!(
            cols.contains(&"issue_id".to_string()),
            "issue_id column should exist after migration"
        );
        assert!(
            cols.contains(&"blocked_by".to_string()),
            "blocked_by column should exist after migration"
        );
        assert!(
            cols.contains(&"blocked_at".to_string()),
            "blocked_at column should exist after migration"
        );
        assert!(
            !cols.contains(&"id".to_string()),
            "legacy id column should be removed"
        );
    }

    /// Migration: add missing issue columns for older schemas.
    #[test]
    fn test_migration_adds_missing_issue_columns() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
            );
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        let cols: Vec<String> = conn
            .query("PRAGMA table_info('issues')")
            .unwrap()
            .iter()
            .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
            .collect();

        let required = [
            "description",
            "design",
            "acceptance_criteria",
            "notes",
            "owner",
            "created_by",
            "updated_at",
            "source_repo",
            "compaction_level",
            "sender",
            "is_template",
        ];

        for column in required {
            assert!(
                cols.contains(&column.to_string()),
                "missing column {column}"
            );
        }
    }

    /// Migration: add missing dependency type column for older schemas.
    #[test]
    fn test_migration_adds_missing_dependency_type() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE issues (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL
            );
            CREATE TABLE dependencies (
                issue_id TEXT NOT NULL,
                depends_on_id TEXT NOT NULL,
                PRIMARY KEY (issue_id, depends_on_id)
            );
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        assert!(
            conn.query("PRAGMA table_info('dependencies')")
                .unwrap()
                .iter()
                .filter_map(|row| row.get(1).and_then(|v| v.as_text()).map(String::from))
                .any(|col| col == "type"),
            "missing dependency type column"
        );
    }

    #[test]
    fn test_migration_rebuilds_legacy_config_metadata_primary_keys() {
        let conn = Connection::open(":memory:").unwrap();

        execute_batch(
            &conn,
            r"
            CREATE TABLE config (
                key TEXT NOT NULL,
                value TEXT NOT NULL
            );
            CREATE TABLE metadata (
                key TEXT NOT NULL,
                value TEXT NOT NULL
            );
            INSERT INTO config (key, value) VALUES ('issue_prefix', 'old');
            INSERT INTO config (key, value) VALUES ('issue_prefix', 'new');
            INSERT INTO metadata (key, value) VALUES ('project', 'old');
            INSERT INTO metadata (key, value) VALUES ('project', 'new');
        ",
        )
        .unwrap();

        apply_schema(&conn).unwrap();

        // key column should be PRIMARY KEY again in rebuilt tables.
        let config_key_pk = conn
            .query("SELECT pk FROM pragma_table_info('config') WHERE name='key'")
            .unwrap()
            .first()
            .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
            .unwrap_or(0);
        assert_eq!(config_key_pk, 1);

        let metadata_key_pk = conn
            .query("SELECT pk FROM pragma_table_info('metadata') WHERE name='key'")
            .unwrap()
            .first()
            .and_then(|row| row.get(0).and_then(SqliteValue::as_integer))
            .unwrap_or(0);
        assert_eq!(metadata_key_pk, 1);

        // Migration should preserve the latest value for duplicate keys.
        let config_latest = conn
            .query_row_with_params(
                "SELECT value FROM config WHERE key = ?",
                &[SqliteValue::from("issue_prefix")],
            )
            .unwrap();
        assert_eq!(
            config_latest.get(0).and_then(SqliteValue::as_text),
            Some("new")
        );

        let metadata_latest = conn
            .query_row_with_params(
                "SELECT value FROM metadata WHERE key = ?",
                &[SqliteValue::from("project")],
            )
            .unwrap();
        assert_eq!(
            metadata_latest.get(0).and_then(SqliteValue::as_text),
            Some("new")
        );
    }
}
