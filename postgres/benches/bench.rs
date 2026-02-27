use criterion::{Criterion, criterion_group, criterion_main};
use postgres::{Client, NoTls};

// spawned: 249us 252us 255us
// local: 214us 216us 219us
fn query_prepared(c: &mut Criterion) {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    let stmt = client.prepare("SELECT $1::INT8").unwrap();

    c.bench_function("query_prepared", move |b| {
        b.iter(|| client.query(&stmt, &[&1i64]).unwrap())
    });
}

criterion_group!(group, query_prepared);
criterion_main!(group);
