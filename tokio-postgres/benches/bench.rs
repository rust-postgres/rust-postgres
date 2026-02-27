use criterion::{Criterion, criterion_group, criterion_main};
use futures_channel::oneshot;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, NoTls};

fn setup() -> (Client, Runtime) {
    let runtime = Runtime::new().unwrap();
    let (client, conn) = runtime
        .block_on(tokio_postgres::connect(
            "host=localhost port=5433 user=postgres",
            NoTls,
        ))
        .unwrap();
    runtime.spawn(async { conn.await.unwrap() });
    (client, runtime)
}

fn query_prepared(c: &mut Criterion) {
    let (client, runtime) = setup();
    let statement = runtime.block_on(client.prepare("SELECT $1::INT8")).unwrap();
    c.bench_function("runtime_block_on", move |b| {
        b.iter(|| {
            runtime
                .block_on(client.query(&statement, &[&1i64]))
                .unwrap()
        })
    });

    let (client, runtime) = setup();
    let statement = runtime.block_on(client.prepare("SELECT $1::INT8")).unwrap();
    c.bench_function("executor_block_on", move |b| {
        b.iter(|| futures_executor::block_on(client.query(&statement, &[&1i64])).unwrap())
    });

    let (client, runtime) = setup();
    let client = Arc::new(client);
    let statement = runtime.block_on(client.prepare("SELECT $1::INT8")).unwrap();
    c.bench_function("spawned", move |b| {
        b.iter_custom(|iters| {
            let (tx, rx) = oneshot::channel();
            let client = client.clone();
            let statement = statement.clone();
            runtime.spawn(async move {
                let start = Instant::now();
                for _ in 0..iters {
                    client.query(&statement, &[&1i64]).await.unwrap();
                }
                tx.send(start.elapsed()).unwrap();
            });
            futures_executor::block_on(rx).unwrap()
        })
    });
}

criterion_group!(benches, query_prepared);
criterion_main!(benches);
