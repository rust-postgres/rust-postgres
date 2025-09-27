use crate::connect;
use futures_util::TryStreamExt;
use std::pin::pin;
use tokio_postgres::binary_copy::{BinaryCopyInWriter, BinaryCopyOutStream};
use tokio_postgres::types::Type;

#[tokio::test]
async fn write_basic() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .await
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .await
        .unwrap();
    let mut writer = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));
    writer.as_mut().write(&[&1i32, &"foobar"]).await.unwrap();
    writer
        .as_mut()
        .write(&[&2i32, &None::<&str>])
        .await
        .unwrap();
    writer.finish().await.unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, Option<&str>>(1), Some("foobar"));
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, Option<&str>>(1), None);
}

#[tokio::test]
async fn write_many_rows() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar TEXT)")
        .await
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .await
        .unwrap();
    let mut writer = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::TEXT]));

    for i in 0..10_000i32 {
        writer
            .as_mut()
            .write(&[&i, &format!("the value for {i}")])
            .await
            .unwrap();
    }

    writer.finish().await.unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .await
        .unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &str>(1), format!("the value for {i}"));
    }
}

#[tokio::test]
async fn write_big_rows() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar BYTEA)")
        .await
        .unwrap();

    let sink = client
        .copy_in("COPY foo (id, bar) FROM STDIN BINARY")
        .await
        .unwrap();
    let mut writer = pin!(BinaryCopyInWriter::new(sink, &[Type::INT4, Type::BYTEA]));

    for i in 0..2i32 {
        writer
            .as_mut()
            .write(&[&i, &vec![i as u8; 128 * 1024]])
            .await
            .unwrap();
    }

    writer.finish().await.unwrap();

    let rows = client
        .query("SELECT id, bar FROM foo ORDER BY id", &[])
        .await
        .unwrap();
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<_, i32>(0), i as i32);
        assert_eq!(row.get::<_, &[u8]>(1), &*vec![i as u8; 128 * 1024]);
    }
}

#[tokio::test]
async fn read_basic() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "
            CREATE TEMPORARY TABLE foo (id INT, bar TEXT);
            INSERT INTO foo (id, bar) VALUES (1, 'foobar'), (2, NULL);
            ",
        )
        .await
        .unwrap();

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .await
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    assert_eq!(rows[0].get::<i32>(0), 1);
    assert_eq!(rows[0].get::<Option<&str>>(1), Some("foobar"));
    assert_eq!(rows[1].get::<i32>(0), 2);
    assert_eq!(rows[1].get::<Option<&str>>(1), None);
}

#[tokio::test]
async fn read_many_rows() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "
            CREATE TEMPORARY TABLE foo (id INT, bar TEXT);
            INSERT INTO foo (id, bar) SELECT i, 'the value for ' || i FROM generate_series(0, 9999) i;"
        )
        .await
        .unwrap();

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .await
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::TEXT])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 10_000);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<i32>(0), i as i32);
        assert_eq!(row.get::<&str>(1), format!("the value for {i}"));
    }
}

#[tokio::test]
async fn read_big_rows() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT, bar BYTEA)")
        .await
        .unwrap();
    for i in 0..2i32 {
        client
            .execute(
                "INSERT INTO foo (id, bar) VALUES ($1, $2)",
                &[&i, &vec![i as u8; 128 * 1024]],
            )
            .await
            .unwrap();
    }

    let stream = client
        .copy_out("COPY foo (id, bar) TO STDIN BINARY")
        .await
        .unwrap();
    let rows = BinaryCopyOutStream::new(stream, &[Type::INT4, Type::BYTEA])
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);

    for (i, row) in rows.iter().enumerate() {
        assert_eq!(row.get::<i32>(0), i as i32);
        assert_eq!(row.get::<&[u8]>(1), &vec![i as u8; 128 * 1024][..]);
    }
}
