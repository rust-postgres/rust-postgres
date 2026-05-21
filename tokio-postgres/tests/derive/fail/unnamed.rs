use tokio_postgres::FromRow;

#[derive(FromRow)]
struct User(i64);

fn main() {}
