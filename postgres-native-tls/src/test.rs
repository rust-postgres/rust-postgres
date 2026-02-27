use futures_util::FutureExt;
use native_tls::{self, Certificate};
use tokio::net::TcpStream;
use tokio_postgres::tls::TlsConnect;

#[cfg(feature = "runtime")]
use crate::MakeTlsConnector;
use crate::{TlsConnector, set_postgresql_alpn};

async fn smoke_test<T>(s: &str, tls: T)
where
    T: TlsConnect<TcpStream>,
    T::Stream: 'static + Send,
{
    let stream = TcpStream::connect("127.0.0.1:5433").await.unwrap();

    let builder = s.parse::<tokio_postgres::Config>().unwrap();
    let (client, connection) = builder.connect_raw(stream, tls).await.unwrap();

    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);

    let stmt = client.prepare("SELECT $1::INT4").await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}

#[tokio::test]
async fn require() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require",
        TlsConnector::new(connector, "localhost"),
    )
    .await;
}

#[tokio::test]
async fn direct() {
    let mut builder = native_tls::TlsConnector::builder();
    builder.add_root_certificate(
        Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
    );
    set_postgresql_alpn(&mut builder);
    let connector = builder.build().unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require sslnegotiation=direct",
        TlsConnector::new(connector, "localhost"),
    )
    .await;
}

#[tokio::test]
async fn prefer() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres",
        TlsConnector::new(connector, "localhost"),
    )
    .await;
}

#[tokio::test]
async fn scram_user() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=scram_user password=password dbname=postgres sslmode=require",
        TlsConnector::new(connector, "localhost"),
    )
    .await;
}

#[tokio::test]
#[cfg(feature = "runtime")]
async fn runtime() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    let connector = MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres sslmode=require",
        connector,
    )
    .await
    .unwrap();
    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);

    let stmt = client.prepare("SELECT $1::INT4").await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}
