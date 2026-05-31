use futures_util::TryStreamExt;
use std::pin::pin;
use tokio::net::TcpStream;
use tokio_postgres::tls::NoTls;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{Client, Config, SimpleQueryMessage};

async fn connect() -> Client {
    let socket = TcpStream::connect("127.0.0.1:5433").await.unwrap();
    let config = "user=pass_user password=password dbname=postgres"
        .parse::<Config>()
        .unwrap();
    let (client, connection) = config.connect_raw(socket, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}

#[tokio::test]
async fn still_exposed_client_methods_work() {
    let mut client = connect().await;

    let messages = client.simple_query("SELECT 1").await.unwrap();
    match &messages[..] {
        [
            SimpleQueryMessage::RowDescription(_),
            SimpleQueryMessage::Row(row),
            SimpleQueryMessage::CommandComplete(1),
        ] => {
            assert_eq!(row.get(0), Some("1"));
        }
        _ => panic!("unexpected simple query response"),
    }

    client
        .batch_execute(
            "
            CREATE TEMPORARY TABLE typed_surface (
                name TEXT,
                age INT
            );
            ",
        )
        .await
        .unwrap();

    let inserted = client
        .execute_typed(
            "INSERT INTO typed_surface (name, age) VALUES ($1, $2), ($3, $4)",
            &[
                (&"alice", Type::TEXT),
                (&20i32, Type::INT4),
                (&"bob", Type::TEXT),
                (&30i32, Type::INT4),
            ],
        )
        .await
        .unwrap();
    assert_eq!(inserted, 2);

    let row = client
        .query_typed_one(
            "SELECT age FROM typed_surface WHERE name = $1",
            &[(&"alice", Type::TEXT)],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 20);

    let row = client
        .query_typed_opt(
            "SELECT age FROM typed_surface WHERE name = $1",
            &[(&"carol", Type::TEXT)],
        )
        .await
        .unwrap();
    assert!(row.is_none());

    let rows = client
        .query_typed("SELECT name, age FROM typed_surface ORDER BY age", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, &str>(0), "alice");
    assert_eq!(rows[1].get::<_, i32>(1), 30);

    let stream = client
        .query_typed_raw(
            "SELECT name FROM typed_surface WHERE age > $1 ORDER BY age",
            std::iter::once((&20i32, Type::INT4)),
        )
        .await
        .unwrap();
    let mut stream = pin!(stream);
    let mut names = Vec::new();
    while let Some(row) = stream.try_next().await.unwrap() {
        names.push(row.get::<_, String>(0));
    }
    assert_eq!(names, ["bob"]);

    let transaction = client.transaction().await.unwrap();
    let updated = transaction
        .execute_typed(
            "UPDATE typed_surface SET age = $1 WHERE name = $2",
            &[(&40i32, Type::INT4), (&"alice", Type::TEXT)],
        )
        .await
        .unwrap();
    assert_eq!(updated, 1);
    transaction.rollback().await.unwrap();

    let row = client
        .query_typed_one(
            "SELECT age FROM typed_surface WHERE name = $1",
            &[(&"alice", Type::TEXT)],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i32>(0), 20);
}

#[tokio::test]
async fn typed_queries_resolve_custom_type_metadata() {
    let client = connect().await;

    client
        .batch_execute(
            "
            CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok', 'happy');
            CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16);
            CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC,
                session pg_temp.session_id
            );
            ",
        )
        .await
        .unwrap();

    let row = client
        .query_typed_one("SELECT NULL::pg_temp.mood", &[])
        .await
        .unwrap();
    let ty = row.columns()[0].type_();
    assert_eq!(ty.name(), "mood");
    assert_eq!(
        ty.kind(),
        &Kind::Enum(vec!["sad".into(), "ok".into(), "happy".into()])
    );

    let row = client
        .query_typed_one("SELECT NULL::pg_temp.inventory_item", &[])
        .await
        .unwrap();
    let ty = row.columns()[0].type_();
    assert_eq!(ty.name(), "inventory_item");
    match ty.kind() {
        Kind::Composite(fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::TEXT);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::INT4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::NUMERIC);
            assert_eq!(fields[3].name(), "session");
            assert_eq!(fields[3].type_().name(), "session_id");
            assert_eq!(fields[3].type_().kind(), &Kind::Domain(Type::BYTEA));
        }
        _ => panic!("unexpected kind"),
    }
}
