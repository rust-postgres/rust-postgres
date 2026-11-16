use futures_util::{FutureExt, join};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::{MakeTlsConnect, NoTlsStream};
use tokio_postgres::{Client, NoTls, Socket};

async fn connect(s: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(s, NoTls).await.unwrap();
    let connection = connection.map(|e| e.unwrap());
    tokio::spawn(connection);

    client
}

async fn smoke_test(s: &str) {
    let client = connect(s).await;

    let stmt = client.prepare("SELECT $1::INT").await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1i32);
}

#[tokio::test]
#[ignore] // FIXME doesn't work with our docker-based tests :(
async fn unix_socket() {
    smoke_test("host=/var/run/postgresql port=5433 user=postgres").await;
}

#[tokio::test]
async fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres").await;
}

#[tokio::test]
async fn multiple_hosts_one_port() {
    smoke_test("host=foobar.invalid,localhost port=5433 user=postgres").await;
}

#[tokio::test]
async fn multiple_hosts_multiple_ports() {
    smoke_test("host=foobar.invalid,localhost port=5432,5433 user=postgres").await;
}

#[tokio::test]
async fn wrong_port_count() {
    tokio_postgres::connect("host=localhost port=5433,5433 user=postgres", NoTls)
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn target_session_attrs_ok() {
    smoke_test("host=localhost port=5433 user=postgres target_session_attrs=read-write").await;
}

#[tokio::test]
async fn target_session_attrs_err() {
    tokio_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write
         options='-c default_transaction_read_only=on'",
        NoTls,
    )
    .await
    .err()
    .unwrap();
}

#[tokio::test]
async fn host_only_ok() {
    let _ = tokio_postgres::connect(
        "host=localhost port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hostaddr_only_ok() {
    let _ = tokio_postgres::connect(
        "hostaddr=127.0.0.1 port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .unwrap();
}

/// A connector that, like many TLS implementations, cannot be built for an empty server name.
struct NamedOnlyTls;

impl MakeTlsConnect<Socket> for NamedOnlyTls {
    type Stream = NoTlsStream;
    type TlsConnect = NoTls;
    type Error = Box<dyn std::error::Error + Sync + Send>;

    fn make_tls_connect(&mut self, domain: &str) -> Result<NoTls, Self::Error> {
        if domain.is_empty() {
            Err("invalid server name".into())
        } else {
            Ok(NoTls)
        }
    }
}

#[tokio::test]
async fn hostaddr_only_prefer_without_server_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    // A server that declines TLS, then reads the startup message.
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut ssl_request = [0; 8];
        stream.read_exact(&mut ssl_request).await?;
        assert_eq!(ssl_request, [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f]);
        stream.write_all(b"N").await?;
        let mut startup = [0; 8];
        stream.read_exact(&mut startup).await?;
        Ok::<_, std::io::Error>(startup[4..] == 196608u32.to_be_bytes())
    });

    // The server closes the connection after the startup message, so the connection fails.
    let _ = tokio_postgres::connect(
        &format!("hostaddr=127.0.0.1 port={port} user=postgres"),
        NamedOnlyTls,
    )
    .await;

    let sent_startup = server
        .await
        .unwrap()
        .expect("the client sent no startup message");
    assert!(sent_startup);
}

#[tokio::test]
async fn hostaddr_and_host_ok() {
    let _ = tokio_postgres::connect(
        "hostaddr=127.0.0.1 host=localhost port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hostaddr_host_mismatch() {
    let _ = tokio_postgres::connect(
        "hostaddr=127.0.0.1,127.0.0.2 host=localhost port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .err()
    .unwrap();
}

#[tokio::test]
async fn hostaddr_host_both_missing() {
    let _ = tokio_postgres::connect(
        "port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .err()
    .unwrap();
}

#[tokio::test]
async fn cancel_query() {
    let client = connect("host=localhost port=5433 user=postgres").await;

    let cancel_token = client.cancel_token();
    let cancel = cancel_token.cancel_query(NoTls);
    let cancel = time::sleep(Duration::from_millis(100)).then(|()| cancel);

    let sleep = client.batch_execute("SELECT pg_sleep(100)");

    match join!(sleep, cancel) {
        (Err(ref e), Ok(())) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
        t => panic!("unexpected return: {:?}", t),
    }
}
