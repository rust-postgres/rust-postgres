use futures_util::{FutureExt, join};
use std::time::Duration;
use tokio::time;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, NoTls};

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
    smoke_test("postgres://postgres@%2Frun%2Fpostgresql:5433/").await;
    smoke_test("postgres://postgres@/?host=/run/postgresql&port=5433").await;

    // Fallback to default socket path when `host` is `None`
    // (on non-unix, error cause: "both host and hostaddr are missing")
    smoke_test("port=5433 user=postgres").await;
    smoke_test("postgres://postgres@/?port=5433").await;

    // Fallback to default socket path when `host` is ""
    // (on non-unix, error cause: "failed to lookup address information: No address associated with hostname")
    smoke_test("host='' port=5433 user=postgres").await;
    smoke_test("postgres://postgres@/?host=&port=5433").await;
    smoke_test("postgres://postgres@:5433").await;
    smoke_test("postgres://postgres@:5433,:5433").await;

    // Previously these would always fail when looking up the empty hostname, and then attempt the
    // intended host. With a fallback path now in place, the empty host is no longer a silent
    // failure and may result in a successful connection to the fallback host path.
    smoke_test("host=,/does/not/exist port=5433 user=postgres").await;
    smoke_test("postgres://postgres@:5433/?host=/does/not/exist").await;
}

#[tokio::test]
async fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres").await;
    smoke_test("postgres://postgres@localhost:5433/").await;
}

#[tokio::test]
async fn multiple_hosts_one_port() {
    smoke_test("host=foobar.invalid,localhost port=5433 user=postgres").await;
    smoke_test("postgres:///?port=5433&host=foobar.invalid&host=localhost&user=postgres").await;
}

#[tokio::test]
async fn multiple_hosts_multiple_ports() {
    smoke_test("host=foobar.invalid,localhost port=5432,5433 user=postgres").await;
    smoke_test("postgres://postgres@foobar.invalid:5432,localhost:5433/").await;
}

#[tokio::test]
async fn wrong_port_count() {
    tokio_postgres::connect("host=localhost port=5433,5433 user=postgres", NoTls)
        .await
        .err()
        .unwrap();

    // An implicit default port is provided when a host (`localhost` here) is provided, resulting
    // in 2 ports and only 1 host.
    tokio_postgres::connect("postgres://localhost/?port=5433&user=postgres", NoTls)
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn target_session_attrs_ok() {
    smoke_test("host=localhost port=5433 user=postgres target_session_attrs=read-write").await;
    smoke_test("postgres://postgres@localhost:5433/?target_session_attrs=read-write").await;
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

    let _ = tokio_postgres::connect(
        "postgres://pass_user:password@localhost:5433/postgres",
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

    let _ = tokio_postgres::connect(
        "postgres:///?hostaddr=127.0.0.1&port=5433&user=pass_user&dbname=postgres&password=password",
        NoTls,
    )
    .await
    .unwrap();

    let _ = tokio_postgres::connect(
        "postgres://pass_user:password@/postgres?hostaddr=127.0.0.1&port=5433",
        NoTls,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hostaddr_with_empty_host_ok() {
    let _ = tokio_postgres::connect(
        "host='' hostaddr=127.0.0.1 port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .unwrap();

    // The `host` is implicitly set to "" because of the `:port` portion is present.
    let _ = tokio_postgres::connect(
        "postgres://pass_user:password@:5433/postgres?hostaddr=127.0.0.1",
        NoTls,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn hostaddr_and_host_ok() {
    let _ = tokio_postgres::connect(
        "hostaddr=127.0.0.1 host=localhost port=5433 user=pass_user dbname=postgres password=password",
        NoTls,
    )
    .await
    .unwrap();

    let _ = tokio_postgres::connect(
        "postgres://pass_user:password@localhost:5433/postgres?hostaddr=127.0.0.1",
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

    let _ = tokio_postgres::connect(
        "postgres://pass_user:password@localhost:5433/postgres?hostaddr=127.0.0.1,127.0.0.2",
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

    let _ = tokio_postgres::connect("postgres://pass_user:password@/postgres?port=5433", NoTls)
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
