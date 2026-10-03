use std::env;
use std::time::{Duration, Instant};
use tokio_postgres::{Client, NoTls};

async fn connect() -> Client {
    let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = env::var("POSTGRES_PORT")
        .unwrap_or_else(|_| "5433".into())
        .parse()
        .unwrap();
    let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".into());
    let password = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| String::new());
    let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".into());

    let config = format!("host={host} port={port} user={user} password={password} dbname={dbname}");
    let (client, conn) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    tokio::spawn(async move { conn.await.unwrap() });
    client
}

/// Insert 1000 rows, verify returned count == 1000 and the table has 1000 rows.
#[tokio::test]
async fn functional_parity_1000_rows() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE bem_parity (id SERIAL, val INT NOT NULL)")
        .await
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO bem_parity (val) VALUES ($1)")
        .await
        .unwrap();

    let rows_affected = client
        .bind_execute_many(&stmt, (0i32..1000).map(|i| [i]))
        .await
        .unwrap();

    assert_eq!(rows_affected, 1000);

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM bem_parity", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1000);
}

/// Passing a param-set with wrong column count returns Err; the connection stays usable.
#[tokio::test]
async fn arity_mismatch_returns_err_and_connection_stays_usable() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE bem_arity (a INT, b INT)")
        .await
        .unwrap();
    let stmt = client
        .prepare("INSERT INTO bem_arity (a, b) VALUES ($1, $2)")
        .await
        .unwrap();

    // Pass only one value where two are expected.
    let err = client
        .bind_execute_many(&stmt, [[1i32]])
        .await
        .expect_err("arity mismatch must return Err");
    assert!(
        err.to_string().contains("parameter"),
        "unexpected error: {err}",
    );

    // The connection must still respond to new requests.
    let val: i32 = client.query_one("SELECT 42", &[]).await.unwrap().get(0);
    assert_eq!(val, 42);
}

/// A CHECK-constraint violation mid-batch surfaces as a database error; the connection recovers.
#[tokio::test]
async fn mid_batch_server_error_and_connection_recovers() {
    let client = connect().await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE bem_error (\
                val TEXT NOT NULL CHECK (val <> 'bad')\
            )",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("INSERT INTO bem_error (val) VALUES ($1)")
        .await
        .unwrap();

    // 100 rows, where index 50 violates the CHECK constraint.
    let values: Vec<String> = (0..100)
        .map(|i| {
            if i == 50 {
                "bad".to_string()
            } else {
                format!("ok{i}")
            }
        })
        .collect();

    let err = client
        .bind_execute_many(&stmt, values.iter().map(|v| [v.as_str()]))
        .await
        .expect_err("constraint violation must return Err");
    assert!(
        err.as_db_error().is_some(),
        "error must be a database (server-side) error, got: {err}",
    );

    // Connection must still be usable after a server error.
    let val: i32 = client.query_one("SELECT 99", &[]).await.unwrap().get(0);
    assert_eq!(val, 99);
}

/// An empty iterator returns Ok(0) without any wire traffic.
#[tokio::test]
async fn empty_iterator_returns_zero() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE bem_empty (val INT)")
        .await
        .unwrap();
    let stmt = client
        .prepare("INSERT INTO bem_empty (val) VALUES ($1)")
        .await
        .unwrap();

    let result = client
        .bind_execute_many(&stmt, std::iter::empty::<[i32; 1]>())
        .await
        .unwrap();
    assert_eq!(result, 0);

    let count: i64 = client
        .query_one("SELECT COUNT(*) FROM bem_empty", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
}

/// 10 000-row batch completes within a generous timeout and returns the correct count.
#[tokio::test]
async fn large_iterator_10k_rows_completes() {
    let client = connect().await;

    client
        .batch_execute("CREATE TEMPORARY TABLE bem_large (val INT NOT NULL)")
        .await
        .unwrap();
    let stmt = client
        .prepare("INSERT INTO bem_large (val) VALUES ($1)")
        .await
        .unwrap();

    let start = Instant::now();
    let rows_affected = client
        .bind_execute_many(&stmt, (0i32..10_000).map(|i| [i]))
        .await
        .unwrap();
    let elapsed = start.elapsed();

    assert_eq!(rows_affected, 10_000);
    assert!(
        elapsed < Duration::from_secs(60),
        "10k-row batch took too long: {elapsed:?}",
    );
}
