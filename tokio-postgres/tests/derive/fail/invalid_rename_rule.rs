use tokio_postgres::FromRow;

#[derive(FromRow)]
#[postgres(rename_all = "invalid")]
struct User {
    id: i64,
}

fn main() {}
