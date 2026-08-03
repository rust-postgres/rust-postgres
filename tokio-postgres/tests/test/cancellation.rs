//! Cancel-safety: dropping an in-flight future must not strand server-side state.
//!
//! Every case in this module has the same shape. A name is allocated and the
//! message that creates the object is written before the future first yields,
//! but the value whose `Drop` emits `Close` — `Statement` for `prepare`, `Portal`
//! for `bind` — is only constructed after the last response has been read.
//! Dropping the future in between leaves the object on the server with nothing
//! left that knows its name: for the rest of the session in the case of a
//! statement, for the rest of the transaction in the case of a portal.
//!
//! Two kinds of state are at risk. **Named objects**: the statement name
//! allocated in `prepare` and the portal name allocated in `bind`, each cleaned
//! up by the `Drop` of the value that owns the name. **Transaction state**:
//! `START TRANSACTION` and `SAVEPOINT`, cleaned up by `Transaction`'s `Drop`.
//!
//! So there are only a handful of distinct defects, however many public methods
//! reach them. The `prepare` one is reached by every method that accepts a query
//! string, because `ToStatement for str` prepares under the hood, and by
//! `prepare` itself through the recursive typeinfo lookup. The tests below cover
//! each defect once at its source plus once through the entry point that makes it
//! matter in practice, rather than once per method that happens to reach it.
//!
//! `Client::transaction` is the one place that already gets this right: it arms a
//! `RollbackIfNotDone` guard around the `BEGIN` and disarms it once the
//! `Transaction` exists. That guard is the shape the others are missing.
//!
//! `pg_prepared_statements` and `pg_cursors` are both session-scoped, so every
//! assertion has to be made on the same client that leaked. Match on the
//! `statement` text and not on the name — names come from process-global
//! counters and so are not predictable across a test run.
//!
//! Only the extended query protocol is affected. `simple_query` and
//! `batch_execute` never name anything, so they have nothing to leak.

use futures_util::poll;
use std::future::Future;
use std::pin::pin;
use tokio_postgres::error::SqlState;
use tokio_postgres::{GenericClient, Transaction};

use crate::{Cancellable, connect, in_transaction};

/// Polls `fut` exactly once — enough to allocate a name and put the message that
/// creates the object on the wire — then drops it without completing it.
///
/// A single poll runs the future synchronously up to its first suspension point,
/// which in every case here is the first read of a response, i.e. strictly after
/// the send. Cancelling this way is deterministic, unlike racing a `timeout`.
async fn cancel_after_first_poll<F: Future>(fut: F) {
    let mut fut = pin!(fut);
    assert!(
        poll!(fut.as_mut()).is_pending(),
        "future resolved on its first poll, so nothing was cancelled and the assertion below \
         would pass vacuously",
    );
}

/// Prepared statements resident in this session whose text is exactly `sql`.
async fn prepared<C>(client: &C, sql: &str) -> i64
where
    C: GenericClient + Sync,
{
    client
        .query_one(
            "SELECT count(*) FROM pg_prepared_statements WHERE statement = $1",
            &[&sql],
        )
        .await
        .unwrap()
        .get(0)
}

/// Open portals in this transaction whose text is exactly `sql`.
async fn cursors(transaction: &Transaction<'_>, sql: &str) -> i64 {
    transaction
        .query_one(
            "SELECT count(*) FROM pg_cursors WHERE statement = $1",
            &[&sql],
        )
        .await
        .unwrap()
        .get(0)
}

/// The control for every other test here: a `prepare` that runs to completion
/// and whose `Statement` is then dropped leaves nothing behind, because `Drop`
/// sends `Close`.
///
/// Without this pairing a nonzero count elsewhere could just mean that
/// `pg_prepared_statements` lags behind the connection.
#[tokio::test]
async fn completed_prepare_is_closed_when_statement_is_dropped() {
    const SQL: &str = "SELECT 1 AS completed_prepare";

    let client = connect("user=postgres").await;

    let statement = client.prepare(SQL).await.unwrap();
    assert_eq!(
        prepared(&client, SQL).await,
        1,
        "statement should be resident on the server while the Statement is alive",
    );

    drop(statement);
    assert_eq!(
        prepared(&client, SQL).await,
        0,
        "dropping a Statement should close it server-side",
    );
}

/// The other boundary: a future dropped *before its first poll* leaks nothing,
/// because `send()` never ran. Worth pinning so the fix is not over-claimed.
#[tokio::test]
async fn unpolled_prepare_leaks_nothing() {
    const SQL: &str = "SELECT 1 AS unpolled_prepare";

    let client = connect("user=postgres").await;

    drop(client.prepare(SQL));

    assert_eq!(
        prepared(&client, SQL).await,
        0,
        "a prepare that was never polled should not have sent anything",
    );
}

#[tokio::test]
async fn cancelled_prepare_does_not_leak_statement() {
    const SQL: &str = "SELECT 1 AS cancelled_prepare";

    let client = connect("user=postgres").await;

    cancel_after_first_poll(client.prepare(SQL)).await;

    assert_eq!(
        prepared(&client, SQL).await,
        0,
        "cancelled prepare leaked a server-side statement",
    );
}

/// `ToStatement for str` routes through `prepare`, so the leak is not confined
/// to explicit `prepare` calls — it fires for the most ordinary use of the
/// crate, a query given as a string literal and wrapped in a timeout.
///
/// This stands in for the whole family. `query`, `query_one` and `query_opt` are
/// wrappers around `query_raw`, and `execute` around `execute_raw`; `query_raw`,
/// `execute_raw`, `copy_in` and `copy_out` are the four leaf methods, and each
/// one opens with the same `into_statement(...).await?`. They do not fail
/// independently of `prepare` — see
/// `cancelled_query_with_prepared_statement_leaks_nothing` below.
#[tokio::test]
async fn cancelled_query_does_not_leak_statement() {
    const SQL: &str = "SELECT 1 AS cancelled_query";

    let client = connect("user=postgres").await;

    cancel_after_first_poll(client.query(SQL, &[])).await;

    assert_eq!(
        prepared(&client, SQL).await,
        0,
        "cancelled query leaked a server-side statement",
    );
}

/// The counterpart that localises the defect: hand `query` a `Statement` that is
/// already prepared and cancellation leaks nothing at all.
///
/// The only name `query` allocates on its own is the *unnamed* portal
/// (`query::encode` passes `""` to `frontend::bind`), which the server replaces
/// on the next bind and destroys at end of transaction, and the statement is
/// owned by the caller's `Statement`. So there is no second bug hiding in the
/// query path — `cancelled_query_does_not_leak_statement` above is the `prepare`
/// bug observed through a different entry point, not an independent one.
#[tokio::test]
async fn cancelled_query_with_prepared_statement_leaks_nothing() {
    const SQL: &str = "SELECT 1 AS cancelled_query_prepared";

    let client = connect("user=postgres").await;
    let statement = client.prepare(SQL).await.unwrap();

    cancel_after_first_poll(client.query(&statement, &[])).await;

    // Still exactly the one the caller owns, and no extra copy stranded.
    assert_eq!(
        prepared(&client, SQL).await,
        1,
        "cancelled query over a prepared Statement should not have touched it",
    );

    drop(statement);
    assert_eq!(
        prepared(&client, SQL).await,
        0,
        "the caller's Statement should still close cleanly after a cancelled query",
    );
}

/// Sibling case: `bind` has the identical shape to `prepare` — name from a
/// counter, `Bind` + `Sync` sent, `BindComplete` awaited, `Portal::new` only
/// afterwards — and `Portal`'s `Drop` is the only emitter of `Close(b'P', …)`.
///
/// Less severe than the statement leak, because a portal dies with its
/// transaction rather than with the session. The statement is prepared up front
/// so that the `bind` is the only thing in flight when the future is dropped.
#[tokio::test]
async fn cancelled_bind_does_not_leak_portal() {
    const SQL: &str = "SELECT 1 AS cancelled_bind";

    let mut client = connect("user=postgres").await;
    let transaction = client.transaction().await.unwrap();
    let statement = transaction.prepare(SQL).await.unwrap();

    cancel_after_first_poll(transaction.bind(&statement, &[])).await;

    assert_eq!(
        cursors(&transaction, SQL).await,
        0,
        "cancelled bind leaked a server-side portal",
    );
}

/// Control for the portal case, for the same reason as
/// `completed_prepare_is_closed_when_statement_is_dropped`.
#[tokio::test]
async fn completed_bind_is_closed_when_portal_is_dropped() {
    const SQL: &str = "SELECT 1 AS completed_bind";

    let mut client = connect("user=postgres").await;
    let transaction = client.transaction().await.unwrap();
    let statement = transaction.prepare(SQL).await.unwrap();

    let portal = transaction.bind(&statement, &[]).await.unwrap();
    assert_eq!(
        cursors(&transaction, SQL).await,
        1,
        "portal should be open while the Portal is alive",
    );

    drop(portal);
    assert_eq!(
        cursors(&transaction, SQL).await,
        0,
        "dropping a Portal should close it server-side",
    );
}

/// `Transaction::savepoint` and the nested `Transaction::transaction` both go
/// through `_savepoint`, which sends `SAVEPOINT <name>`, awaits it, and only then
/// builds the nested `Transaction` that owns the `ROLLBACK TO` in its `Drop`.
/// Cancel in between and the savepoint stays established in the enclosing
/// transaction with nothing tracking it.
///
/// Scoped to the transaction rather than the session, like the portal case, but
/// it also desynchronises the depth counter: `_savepoint` derives the next name
/// from `self.savepoint`, which the cancelled call never updated, so the next
/// `savepoint()` reuses the same name and silently shadows the stranded one.
#[tokio::test]
async fn cancelled_savepoint_does_not_leak_savepoint() {
    let mut client = connect("user=postgres").await;
    let mut transaction = client.transaction().await.unwrap();

    cancel_after_first_poll(transaction.savepoint("cancelled_savepoint")).await;

    // `ROLLBACK TO` succeeds only against a savepoint that actually exists, so
    // this is a direct probe: an error means nothing was left behind.
    let error = transaction
        .batch_execute("ROLLBACK TO cancelled_savepoint")
        .await
        .expect_err("cancelled savepoint leaked a server-side savepoint");

    assert_eq!(
        error.code(),
        // 3B001, "invalid savepoint specification".
        Some(&SqlState::S_E_INVALID_SPECIFICATION),
        "expected the savepoint to be absent, got a different failure",
    );
}

/// `TransactionBuilder::start` has the same shape as `Client::transaction` but,
/// unlike it, is the one that got the `RollbackIfNotDone` guard — see upstream
/// commit a0b2d701, "Fix cancellation of TransactionBuilder::start". This is a
/// regression guard for that fix rather than a new finding.
#[tokio::test]
async fn cancelled_transaction_builder_start_does_not_leak_transaction() {
    let mut client = connect("user=postgres").await;

    cancel_after_first_poll(client.build_transaction().start()).await;

    assert!(
        !in_transaction(&client).await,
        "cancelled TransactionBuilder::start left the session inside a transaction",
    );
}

/// Preparing a query whose types are not built in makes `prepare` call
/// `get_type`, which recursively prepares the typeinfo query. That nested
/// `prepare` can be cancelled too, and it leaks in exactly the same way — so a
/// fix has to cover the recursion, not just the outermost call.
///
/// The nested prepare is several suspension points deep, and precisely how many
/// depends on how the connection task batches responses onto the channel, so
/// there is no single poll count to aim at. Sweeping a budget of polls covers
/// every reachable cancellation point without depending on that batching.
///
/// The assertion is on *duplicates* rather than on absence, because a typeinfo
/// statement that was prepared successfully is legitimately retained by the
/// client's cache. Re-preparing after the cancelled attempt makes a leak show up
/// as a second copy of the same text: one owned by the cache, one stranded.
#[tokio::test]
async fn cancelled_typeinfo_lookup_does_not_leak_statement() {
    // A domain over int4 that exists in every database and that `Type::from_oid`
    // does not know, so the lookup has to go to the server.
    const SQL: &str = "SELECT $1::information_schema.cardinal_number";
    const TYPEINFO: &str = "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, t.typbasetype, \
                            n.nspname, t.typrelid\nFROM pg_catalog.pg_type t\n";

    let mut cancelled_at_least_once = false;

    for polls_left in 1..=12 {
        let client = connect("user=postgres").await;

        let outcome = Cancellable {
            fut: client.prepare(SQL),
            polls_left,
        }
        .await;
        cancelled_at_least_once |= outcome.is_none();

        // Re-prepare so that the typeinfo statement is definitely present and
        // definitely owned by the client's cache. The outer statement is not
        // asserted on here — that is `cancelled_prepare_does_not_leak_statement`'s
        // job, and leaving it out keeps this test about the recursion alone.
        let statement = client.prepare(SQL).await.unwrap();

        let typeinfo: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_prepared_statements WHERE statement LIKE $1",
                &[&format!("{TYPEINFO}%")],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            typeinfo <= 1,
            "prepare cancelled after {polls_left} poll(s) leaked {} typeinfo statement(s)",
            typeinfo - 1,
        );

        drop(statement);
    }

    assert!(
        cancelled_at_least_once,
        "no poll budget in the sweep actually cancelled the prepare, so the test is vacuous",
    );
}
