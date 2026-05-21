use tokio_postgres::FromRow;

#[derive(FromRow)]
struct User<'a> {
    name: &'a str,
}

async fn load(client: &tokio_postgres::Client) -> Result<User<'_>, tokio_postgres::Error> {
    client.query_one_as::<User<'_>>("SELECT name FROM users", &[]).await
}

fn main() {}
