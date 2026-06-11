use tokio_postgres::FromRow;

#[derive(FromRow)]
#[postgres(rename_all = 1)]
struct User {
    id: i64,
}

fn main() {}
