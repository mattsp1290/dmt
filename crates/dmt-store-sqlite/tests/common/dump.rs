use sqlx::{
    Column, Connection, Row, SqliteConnection, TypeInfo, ValueRef, sqlite::SqliteConnectOptions,
};
use std::path::Path;

pub const TABLES: [(&str, &str, &str); 7] = [
    (
        "dmt_schema_meta",
        "SELECT * FROM dmt_schema_meta ORDER BY key",
        "SELECT name FROM pragma_table_info('dmt_schema_meta') ORDER BY cid",
    ),
    (
        "dmt_graphs",
        "SELECT * FROM dmt_graphs ORDER BY id,version",
        "SELECT name FROM pragma_table_info('dmt_graphs') ORDER BY cid",
    ),
    (
        "dmt_runs",
        "SELECT * FROM dmt_runs ORDER BY id",
        "SELECT name FROM pragma_table_info('dmt_runs') ORDER BY cid",
    ),
    (
        "dmt_events",
        "SELECT * FROM dmt_events ORDER BY run_id,seq",
        "SELECT name FROM pragma_table_info('dmt_events') ORDER BY cid",
    ),
    (
        "dmt_tasks",
        "SELECT * FROM dmt_tasks ORDER BY id",
        "SELECT name FROM pragma_table_info('dmt_tasks') ORDER BY cid",
    ),
    (
        "dmt_joins",
        "SELECT * FROM dmt_joins ORDER BY id",
        "SELECT name FROM pragma_table_info('dmt_joins') ORDER BY cid",
    ),
    (
        "dmt_signals",
        "SELECT * FROM dmt_signals ORDER BY id",
        "SELECT name FROM pragma_table_info('dmt_signals') ORDER BY cid",
    ),
];
pub async fn connection(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(path).read_only(true))
        .await
        .unwrap()
}
pub async fn dump(path: &Path) -> Vec<String> {
    let mut conn = connection(path).await;
    let mut output = Vec::new();
    for (table, statement, _) in TABLES {
        let rows = sqlx::query(statement).fetch_all(&mut conn).await.unwrap();
        for row in rows {
            let mut columns = Vec::new();
            for column in row.columns() {
                let index = column.ordinal();
                let raw = row.try_get_raw(index).unwrap();
                let value = if raw.is_null() {
                    "null".into()
                } else {
                    match raw.type_info().name() {
                        "INTEGER" => row.get::<i64, _>(index).to_string(),
                        "TEXT" => serde_json::to_string(&row.get::<String, _>(index)).unwrap(),
                        other => panic!("unsupported type {other} for {table}.{}", column.name()),
                    }
                };
                columns.push(format!("{}={value}", column.name()));
            }
            output.push(format!("{table}:{}", columns.join(",")));
        }
    }
    conn.close().await.unwrap();
    output
}
