use tokio_postgres::FromRow;

#[derive(FromRow)]
#[postgres(name = "users")]
struct User {
    id: i64,
}

fn main() {}
