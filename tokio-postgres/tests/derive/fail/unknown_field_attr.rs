use tokio_postgres::FromRow;

#[derive(FromRow)]
struct User {
    #[postgres(foo = "bar")]
    id: i64,
}

fn main() {}
