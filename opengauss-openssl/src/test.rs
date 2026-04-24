use futures_util::FutureExt;
use openssl::ssl::{SslConnector, SslMethod};
use tokio::net::TcpStream;
use tokio_opengauss::tls::TlsConnect;

use super::*;

async fn smoke_test<T>(s: &str, tls: T)
where
    T: TlsConnect<TcpStream>,
    T::Stream: 'static + Send,
{
    let stream = TcpStream::connect("127.0.0.1:5433").await.unwrap();

    let builder = s.parse::<tokio_opengauss::Config>().unwrap();
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
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn direct() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    set_postgresql_alpn(&mut builder).unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require sslnegotiation=direct",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn prefer() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=ssl_user dbname=postgres",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn scram_user() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=scram_user password=password dbname=postgres sslmode=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn require_channel_binding_err() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    let connector = TlsConnector::new(ctx.configure().unwrap(), "localhost");

    let stream = TcpStream::connect("127.0.0.1:5433").await.unwrap();
    let builder = "user=pass_user password=password dbname=postgres channel_binding=require"
        .parse::<tokio_opengauss::Config>()
        .unwrap();
    builder.connect_raw(stream, connector).await.err().unwrap();
}

#[tokio::test]
async fn require_channel_binding_ok() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=scram_user password=password dbname=postgres channel_binding=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
#[cfg(feature = "runtime")]
async fn runtime() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_opengauss::connect(
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
