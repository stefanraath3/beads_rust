//! Internal database adapter seam for the storage layer.
//!
//! This module isolates the rest of the codebase from the concrete database
//! backend. The rest of the project talks only to these wrapper types, which
//! lets us swap the implementation without rewriting the storage logic.

use std::rc::Rc;

use rusqlite::params_from_iter;
use rusqlite::types::{Value as RawValue, ValueRef};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbErrorKind {
    QueryReturnedNoRows,
    Transient,
    UniqueViolation,
    Other,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct DbError {
    kind: DbErrorKind,
    message: String,
}

impl DbError {
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(self.kind, DbErrorKind::Transient)
    }

    #[must_use]
    pub const fn is_query_returned_no_rows(&self) -> bool {
        matches!(self.kind, DbErrorKind::QueryReturnedNoRows)
    }

    #[must_use]
    pub fn unique_violation(columns: impl Into<String>) -> Self {
        let columns = columns.into();
        Self {
            kind: DbErrorKind::UniqueViolation,
            message: format!("UNIQUE constraint failed: {columns}"),
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            kind: DbErrorKind::Other,
            message: format!("internal error: {message}"),
        }
    }

    #[must_use]
    fn query_returned_no_rows() -> Self {
        Self {
            kind: DbErrorKind::QueryReturnedNoRows,
            message: "query returned no rows".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

#[derive(Debug)]
pub struct Connection {
    inner: Rc<rusqlite::Connection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    values: Vec<SqliteValue>,
}

pub struct PreparedStatement {
    conn: Rc<rusqlite::Connection>,
    sql: String,
}

impl Connection {
    pub fn open(path: impl Into<String>) -> std::result::Result<Self, DbError> {
        let path = path.into();
        let inner = if path == ":memory:" {
            rusqlite::Connection::open_in_memory()
        } else {
            rusqlite::Connection::open(path)
        }
        .map_err(DbError::from)?;

        Ok(Self {
            inner: Rc::new(inner),
        })
    }

    pub fn query(&self, sql: &str) -> std::result::Result<Vec<Row>, DbError> {
        query_all(&self.inner, sql, &[])
    }

    pub fn query_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<Vec<Row>, DbError> {
        query_all(&self.inner, sql, params)
    }

    pub fn query_row(&self, sql: &str) -> std::result::Result<Row, DbError> {
        query_one(&self.inner, sql, &[])
    }

    pub fn query_row_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<Row, DbError> {
        query_one(&self.inner, sql, params)
    }

    pub fn execute(&self, sql: &str) -> std::result::Result<usize, DbError> {
        execute_count(&self.inner, sql, &[])
    }

    pub fn execute_with_params(
        &self,
        sql: &str,
        params: &[SqliteValue],
    ) -> std::result::Result<usize, DbError> {
        execute_count(&self.inner, sql, params)
    }

    pub fn execute_batch(&self, sql: &str) -> std::result::Result<(), DbError> {
        self.inner.execute_batch(sql).map_err(DbError::from)
    }

    pub fn prepare(&self, sql: &str) -> std::result::Result<PreparedStatement, DbError> {
        Ok(PreparedStatement {
            conn: Rc::clone(&self.inner),
            sql: sql.to_string(),
        })
    }
}

impl PreparedStatement {
    pub fn query(&self) -> std::result::Result<Vec<Row>, DbError> {
        query_all(&self.conn, &self.sql, &[])
    }

    pub fn query_with_params(
        &self,
        params: &[SqliteValue],
    ) -> std::result::Result<Vec<Row>, DbError> {
        query_all(&self.conn, &self.sql, params)
    }

    pub fn query_row(&self) -> std::result::Result<Row, DbError> {
        query_one(&self.conn, &self.sql, &[])
    }

    pub fn query_row_with_params(
        &self,
        params: &[SqliteValue],
    ) -> std::result::Result<Row, DbError> {
        query_one(&self.conn, &self.sql, params)
    }

    pub fn execute(&self) -> std::result::Result<usize, DbError> {
        execute_count(&self.conn, &self.sql, &[])
    }

    pub fn execute_with_params(
        &self,
        params: &[SqliteValue],
    ) -> std::result::Result<usize, DbError> {
        execute_count(&self.conn, &self.sql, params)
    }

    #[must_use]
    pub fn explain(&self) -> String {
        format!("EXPLAIN unavailable in adapter mode: {}", self.sql)
    }
}

impl Row {
    #[must_use]
    pub fn values(&self) -> &[SqliteValue] {
        &self.values
    }

    #[must_use]
    pub fn get(&self, index: usize) -> Option<&SqliteValue> {
        self.values.get(index)
    }
}

impl SqliteValue {
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    #[must_use]
    pub const fn as_integer(&self) -> Option<i64> {
        match self {
            Self::Integer(value) => Some(*value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            Self::Float(value) => Some(*value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Self::Text(value) => Some(value),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Self::Blob(value) => Some(value),
            _ => None,
        }
    }
}

impl From<&str> for SqliteValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<String> for SqliteValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<i32> for SqliteValue {
    fn from(value: i32) -> Self {
        Self::Integer(i64::from(value))
    }
}

impl From<i64> for SqliteValue {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl From<f64> for SqliteValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<Vec<u8>> for SqliteValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Blob(value)
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(value: rusqlite::Error) -> Self {
        let message = value.to_string();
        let kind = if matches!(value, rusqlite::Error::QueryReturnedNoRows) {
            DbErrorKind::QueryReturnedNoRows
        } else if message.starts_with("UNIQUE constraint failed:") {
            DbErrorKind::UniqueViolation
        } else if message.contains("database is locked") || message.contains("database is busy") {
            DbErrorKind::Transient
        } else {
            DbErrorKind::Other
        };

        Self { kind, message }
    }
}

fn query_all(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> std::result::Result<Vec<Row>, DbError> {
    let mut stmt = conn.prepare(sql).map_err(DbError::from)?;
    let raw_params = to_backend_values(params);
    let mut rows = stmt
        .query(params_from_iter(raw_params))
        .map_err(DbError::from)?;

    let mut collected = Vec::new();
    while let Some(row) = rows.next().map_err(DbError::from)? {
        collected.push(row_from_rusqlite(row)?);
    }

    Ok(collected)
}

fn query_one(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> std::result::Result<Row, DbError> {
    let rows = query_all(conn, sql, params)?;
    rows.into_iter()
        .next()
        .ok_or_else(DbError::query_returned_no_rows)
}

fn execute_count(
    conn: &rusqlite::Connection,
    sql: &str,
    params: &[SqliteValue],
) -> std::result::Result<usize, DbError> {
    let mut stmt = conn.prepare(sql).map_err(DbError::from)?;
    if stmt.column_count() > 0 {
        return Ok(query_all(conn, sql, params)?.len());
    }

    let raw_params = to_backend_values(params);
    stmt.execute(params_from_iter(raw_params)).map_err(DbError::from)
}

fn row_from_rusqlite(row: &rusqlite::Row<'_>) -> std::result::Result<Row, DbError> {
    let mut values = Vec::with_capacity(row.as_ref().column_count());
    for index in 0..row.as_ref().column_count() {
        values.push(value_from_ref(row.get_ref(index).map_err(DbError::from)?));
    }
    Ok(Row { values })
}

fn value_from_ref(value: ValueRef<'_>) -> SqliteValue {
    match value {
        ValueRef::Null => SqliteValue::Null,
        ValueRef::Integer(value) => SqliteValue::Integer(value),
        ValueRef::Real(value) => SqliteValue::Float(value),
        ValueRef::Text(value) => SqliteValue::Text(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec()),
    }
}

fn to_backend_values(values: &[SqliteValue]) -> Vec<RawValue> {
    values.iter().map(to_backend_value).collect()
}

fn to_backend_value(value: &SqliteValue) -> RawValue {
    match value {
        SqliteValue::Null => RawValue::Null,
        SqliteValue::Integer(value) => RawValue::Integer(*value),
        SqliteValue::Float(value) => RawValue::Real(*value),
        SqliteValue::Text(value) => RawValue::Text(value.clone()),
        SqliteValue::Blob(value) => RawValue::Blob(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::Connection;

    #[test]
    fn execute_batch_handles_semicolons_inside_string_literals() {
        let conn = Connection::open(":memory:").expect("open in-memory db");

        conn.execute_batch(
            "CREATE TABLE t (v TEXT NOT NULL);
             INSERT INTO t (v) VALUES ('alpha;beta');
             INSERT INTO t (v) VALUES ('gamma');",
        )
        .expect("execute batch");

        let rows = conn
            .query("SELECT v FROM t ORDER BY rowid")
            .expect("read inserted rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(0).and_then(|v| v.as_text()), Some("alpha;beta"));
        assert_eq!(rows[1].get(0).and_then(|v| v.as_text()), Some("gamma"));
    }
}
