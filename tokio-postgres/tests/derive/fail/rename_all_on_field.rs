use tokio_postgres::FromRow;

#[derive(FromRow)]
struct User {
    #[postgres(rename_all = "camelCase")]
    id: i64,
}

fn main() {}
