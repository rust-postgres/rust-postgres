use futures_util::{future, StreamExt, TryStreamExt};
use tokio_postgres::{error::SqlState, Client, SimpleQueryMessage, SimpleQueryRow};

async fn q(client: &Client, query: &str) -> Vec<SimpleQueryRow> {
    let msgs = client.simple_query(query).await.unwrap();

    msgs.into_iter()
        .filter_map(|msg| match msg {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn copy_both_error() {
    let client = crate::connect("user=postgres replication=database").await;

    let err = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT undefined LOGICAL 0000/0000")
        .await
        .err()
        .unwrap();

    assert_eq!(err.code(), Some(&SqlState::UNDEFINED_OBJECT));

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}

#[tokio::test]
async fn copy_both_stream_error() {
    let client = crate::connect("user=postgres replication=true").await;

    q(&client, "CREATE_REPLICATION_SLOT err2 PHYSICAL").await;

    // This will immediately error out after entering CopyBoth mode
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT err2 PHYSICAL FFFF/FFFF")
        .await
        .unwrap();

    let mut msgs: Vec<_> = duplex_stream.collect().await;
    let result = msgs.pop().unwrap();
    assert_eq!(msgs.len(), 0);
    assert!(result.unwrap_err().as_db_error().is_some());

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "DROP_REPLICATION_SLOT err2").await.len(), 0);
}

#[tokio::test]
async fn copy_both_stream_error_sync() {
    let client = crate::connect("user=postgres replication=database").await;

    q(&client, "CREATE_REPLICATION_SLOT err1 TEMPORARY PHYSICAL").await;

    // This will immediately error out after entering CopyBoth mode
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>("START_REPLICATION SLOT err1 PHYSICAL FFFF/FFFF")
        .await
        .unwrap();

    // Immediately close our sink to send a CopyDone before receiving the ErrorResponse
    drop(duplex_stream);

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}

#[tokio::test]
async fn copy_both() {
    let client = crate::connect("user=postgres replication=database").await;

    q(&client, "DROP TABLE IF EXISTS replication").await;
    q(&client, "CREATE TABLE replication (i text)").await;

    let slot_query = "CREATE_REPLICATION_SLOT slot TEMPORARY LOGICAL \"test_decoding\"";
    let lsn = q(&client, slot_query).await[0]
        .get("consistent_point")
        .unwrap()
        .to_owned();

    // We will attempt to read this from the other end
    q(&client, "BEGIN").await;
    let xid = q(&client, "SELECT txid_current()").await[0]
        .get("txid_current")
        .unwrap()
        .to_owned();
    q(&client, "INSERT INTO replication VALUES ('processed')").await;
    q(&client, "COMMIT").await;

    // Insert a second row to generate unprocessed messages in the stream
    q(&client, "INSERT INTO replication VALUES ('ignored')").await;

    let query = format!("START_REPLICATION SLOT slot LOGICAL {}", lsn);
    let duplex_stream = client
        .copy_both_simple::<bytes::Bytes>(&query)
        .await
        .unwrap();

    let expected = vec![
        format!("BEGIN {}", xid),
        "table public.replication: INSERT: i[text]:'processed'".to_string(),
        format!("COMMIT {}", xid),
    ];

    let actual: Vec<_> = duplex_stream
        // Process only XLogData messages
        .try_filter(|buf| future::ready(buf[0] == b'w'))
        // Playback the stream until the first expected message
        .try_skip_while(|buf| future::ready(Ok(!buf.ends_with(expected[0].as_ref()))))
        // Take only the expected number of messsage, the rest will be discarded by tokio_postgres
        .take(expected.len())
        .try_collect()
        .await
        .unwrap();

    for (msg, ending) in actual.into_iter().zip(expected.into_iter()) {
        assert!(msg.ends_with(ending.as_ref()));
    }

    // Ensure we can continue issuing queries
    assert_eq!(q(&client, "SELECT 1").await[0].get(0), Some("1"));
}
