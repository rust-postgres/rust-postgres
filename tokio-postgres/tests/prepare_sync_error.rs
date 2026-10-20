//! An error the server sends after a preparation's description, before its `ReadyForQuery`, is
//! returned by `prepare`.
//!
//! PostgreSQL keeps `statement_timeout` armed from the `Parse` until the `Sync` of a preparation
//! (only a completed `Execute` disarms it). When it fires after the `Describe` has been answered,
//! the answer is `ParseComplete`, `ParameterDescription`, `RowDescription`/`NoData`,
//! `ErrorResponse`, `ReadyForQuery`, and the transaction is aborted. These tests replay that
//! answer from a scripted server, so they need no database and no timing.

use std::future::Future;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
use tokio::task::JoinHandle;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, Config, NoTls};

/// One backend message: tag, length (itself included), body.
fn message(tag: u8, body: &[u8]) -> Vec<u8> {
    let len = i32::try_from(body.len() + 4).unwrap();
    let mut out = vec![tag];
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

/// One frontend message after the startup: its tag and body.
async fn read_message(server: &mut DuplexStream) -> (u8, Vec<u8>) {
    let tag = server.read_u8().await.unwrap();
    let len = usize::try_from(server.read_i32().await.unwrap()).unwrap();
    let mut body = vec![0; len - 4];
    server.read_exact(&mut body).await.unwrap();
    (tag, body)
}

/// The `ErrorResponse` PostgreSQL sends when `statement_timeout` fires.
fn statement_timeout_error() -> Vec<u8> {
    let mut fields = Vec::new();
    for (code, value) in [
        (b'S', "ERROR"),
        (b'V', "ERROR"),
        (b'C', "57014"),
        (b'M', "canceling statement due to statement timeout"),
        (b'R', "ProcessInterrupts"),
    ] {
        fields.push(code);
        fields.extend_from_slice(value.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    message(b'E', &fields)
}

/// Read the startup message and answer it: authentication done, ready for queries.
async fn startup(server: &mut DuplexStream) {
    let len = usize::try_from(server.read_i32().await.unwrap()).unwrap();
    let mut startup = vec![0; len - 4];
    server.read_exact(&mut startup).await.unwrap();
    let mut out = message(b'R', &0i32.to_be_bytes());
    out.extend(message(b'Z', b"I"));
    server.write_all(&out).await.unwrap();
}

/// Read one request up to and including its `Sync`, appending each message's tag to `tags`.
async fn read_request(server: &mut DuplexStream, tags: &mut Vec<u8>) {
    loop {
        let (tag, _) = read_message(server).await;
        tags.push(tag);
        if tag == b'S' {
            return;
        }
    }
}

/// The end of a script: the `Close` and `Sync` the client sends for the statement the server
/// parsed (answered in the failed transaction), its `ROLLBACK` and its `Terminate`.
async fn finish(server: &mut DuplexStream, tags: &mut Vec<u8>) {
    // check the Close at once: without it the next message is the ROLLBACK, and waiting for a
    // Sync would hang
    let (tag, _) = read_message(server).await;
    tags.push(tag);
    assert_eq!(
        tag, b'C',
        "the client closes the statement the server parsed (tags so far {tags:?})"
    );
    read_request(server, tags).await;
    let mut out = message(b'3', b"");
    out.extend(message(b'Z', b"E"));
    server.write_all(&out).await.unwrap();
    let (tag, body) = read_message(server).await;
    tags.push(tag);
    assert_eq!(body, b"ROLLBACK\0", "the next request is the ROLLBACK");
    let mut out = message(b'C', b"ROLLBACK\0");
    out.extend(message(b'Z', b"I"));
    server.write_all(&out).await.unwrap();
    let (tag, _) = read_message(server).await;
    tags.push(tag);
}

/// Answers a preparation with its description, then the statement timeout and `ReadyForQuery`
/// in a failed transaction.
async fn serve_timeout_at_sync(mut server: DuplexStream) -> Vec<u8> {
    startup(&mut server).await;
    let mut tags = Vec::new();
    read_request(&mut server, &mut tags).await;
    let mut out = message(b'1', b"");
    out.extend(message(b't', &0i16.to_be_bytes()));
    out.extend(message(b'n', b""));
    out.extend(statement_timeout_error());
    out.extend(message(b'Z', b"E"));
    server.write_all(&out).await.unwrap();
    finish(&mut server, &mut tags).await;
    tags
}

/// A type the client does not know, so it looks it up with its type query.
const UNKNOWN_TYPE: u32 = 900_001;

/// One field of a `RowDescription`: name, table, column, type, size, modifier, format.
fn field(name: &str, type_oid: u32, size: i16) -> Vec<u8> {
    let mut out = name.as_bytes().to_vec();
    out.push(0);
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    out.extend_from_slice(&type_oid.to_be_bytes());
    out.extend_from_slice(&size.to_be_bytes());
    out.extend_from_slice(&(-1i32).to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    out
}

/// One value of a binary `DataRow`, or NULL.
fn value(row: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        Some(bytes) => {
            row.extend_from_slice(&i32::try_from(bytes.len()).unwrap().to_be_bytes());
            row.extend_from_slice(bytes);
        }
        None => row.extend_from_slice(&(-1i32).to_be_bytes()),
    }
}

/// The prepared statement has a parameter of an unknown type: the client prepares and runs its
/// type query, and the server sends the type's row, then the statement timeout, then
/// `ReadyForQuery` in a failed transaction.
async fn serve_timeout_after_the_type_row(mut server: DuplexStream) -> Vec<u8> {
    const NAME: u32 = 19;
    const CHAR: u32 = 18;
    const OID: u32 = 26;
    startup(&mut server).await;
    let mut tags = Vec::new();
    // the caller's statement: one parameter of the unknown type, no columns
    read_request(&mut server, &mut tags).await;
    let mut parameters = 1i16.to_be_bytes().to_vec();
    parameters.extend_from_slice(&UNKNOWN_TYPE.to_be_bytes());
    let mut out = message(b'1', b"");
    out.extend(message(b't', &parameters));
    out.extend(message(b'n', b""));
    out.extend(message(b'Z', b"I"));
    server.write_all(&out).await.unwrap();
    // the type query's preparation: one oid parameter, seven columns
    read_request(&mut server, &mut tags).await;
    let mut parameters = 1i16.to_be_bytes().to_vec();
    parameters.extend_from_slice(&OID.to_be_bytes());
    let mut columns = 7i16.to_be_bytes().to_vec();
    for (name, oid, size) in [
        ("typname", NAME, 64),
        ("typtype", CHAR, 1),
        ("typelem", OID, 4),
        ("rngsubtype", OID, 4),
        ("typbasetype", OID, 4),
        ("nspname", NAME, 64),
        ("typrelid", OID, 4),
    ] {
        columns.extend(field(name, oid, size));
    }
    let mut out = message(b'1', b"");
    out.extend(message(b't', &parameters));
    out.extend(message(b'T', &columns));
    out.extend(message(b'Z', b"I"));
    server.write_all(&out).await.unwrap();
    // its execution: the row (binary), then the timeout
    read_request(&mut server, &mut tags).await;
    let mut row = 7i16.to_be_bytes().to_vec();
    value(&mut row, Some(b"mystery"));
    value(&mut row, Some(b"b"));
    value(&mut row, Some(&0u32.to_be_bytes()));
    value(&mut row, None);
    value(&mut row, Some(&0u32.to_be_bytes()));
    value(&mut row, Some(b"public"));
    value(&mut row, Some(&0u32.to_be_bytes()));
    let mut out = message(b'2', b"");
    out.extend(message(b'D', &row));
    out.extend(statement_timeout_error());
    out.extend(message(b'Z', b"E"));
    server.write_all(&out).await.unwrap();
    finish(&mut server, &mut tags).await;
    tags
}

type Driver = JoinHandle<Result<(), tokio_postgres::Error>>;

/// A client on a scripted server: the client, its connection task and the server task, which
/// returns the tags of the messages it read.
async fn scripted<F>(serve: impl FnOnce(DuplexStream) -> F) -> (Client, Driver, JoinHandle<Vec<u8>>)
where
    F: Future<Output = Vec<u8>> + Send + 'static,
{
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);
    let server = tokio::spawn(serve(server_side));
    let mut config = Config::new();
    config.user("postgres").dbname("postgres");
    let (client, connection) = config.connect_raw(client_side, NoTls).await.unwrap();
    (client, tokio::spawn(connection), server)
}

/// A script that waits for a message the client never sends fails the test instead of hanging.
async fn within_guard<T>(exchange: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(30), exchange)
        .await
        .expect("the scripted exchange finishes")
}

/// Send the `ROLLBACK`, drop the client and return the tags the server read. The server's result
/// is checked first, so a failed script assertion is what gets reported.
async fn end(client: Client, driver: Driver, server: JoinHandle<Vec<u8>>) -> Vec<u8> {
    let rolled_back = client.batch_execute("ROLLBACK").await;
    drop(client);
    let tags = server.await.unwrap();
    rolled_back.unwrap();
    driver.await.unwrap().unwrap();
    tags
}

fn assert_statement_timeout(error: &tokio_postgres::Error) {
    assert_eq!(error.code(), Some(&SqlState::QUERY_CANCELED));
    let db = error.as_db_error().unwrap();
    assert_eq!(db.severity(), "ERROR");
    assert_eq!(db.routine(), Some("ProcessInterrupts"));
}

#[tokio::test]
async fn prepare_returns_an_error_sent_before_ready_for_query() {
    let tags = within_guard(async {
        let (client, driver, server) = scripted(serve_timeout_at_sync).await;
        let error = client.prepare("SELECT 1").await.unwrap_err();
        assert_statement_timeout(&error);
        end(client, driver, server).await
    })
    .await;
    // Parse, Describe, Sync; Close and Sync for the parsed statement; the ROLLBACK; Terminate
    assert_eq!(tags, b"PDSCSQX".to_vec());
}

#[tokio::test]
async fn type_lookup_returns_an_error_sent_after_the_row() {
    let tags = within_guard(async {
        let (client, driver, server) = scripted(serve_timeout_after_the_type_row).await;
        let error = client.prepare("SELECT $1").await.unwrap_err();
        assert_statement_timeout(&error);
        end(client, driver, server).await
    })
    .await;
    // the caller's preparation, the type query's preparation and execution, the Close of the
    // caller's statement, the ROLLBACK and the Terminate
    assert_eq!(tags, b"PDSPDSBESCSQX".to_vec());
}
