use tokio_postgres::{FromRow, GenericClient};

#[derive(FromRow)]
struct User {
    id: i64,
    name: String,
}

#[derive(FromRow)]
struct Renamed {
    #[postgres(name = "user_id")]
    id: i64,
}

#[derive(FromRow)]
#[postgres(rename_all = "camelCase")]
struct AuditLog {
    actor_id: i64,
}

#[derive(FromRow)]
struct Borrowed<'a> {
    name: &'a str,
    data: &'a [u8],
}

#[derive(FromRow)]
struct Generic<T> {
    value: T,
}

struct Wrapper<T>(T);

impl<'a, T> tokio_postgres::types::FromSql<'a> for Wrapper<T> {
    fn from_sql(
        _: &tokio_postgres::types::Type,
        _: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        unimplemented!()
    }

    fn accepts(_: &tokio_postgres::types::Type) -> bool {
        true
    }
}

#[derive(FromRow)]
struct WrappedGeneric<T> {
    value: Wrapper<T>,
}

fn assert_from_row<'a, T>()
where
    T: FromRow<'a>,
{
}

fn assert_generic<'a, T>()
where
    T: tokio_postgres::types::FromSql<'a>,
    Generic<T>: FromRow<'a>,
{
}

fn assert_wrapped_generic<'a, T>()
where
    WrappedGeneric<T>: FromRow<'a>,
{
}

async fn assert_client_methods(
    client: &tokio_postgres::Client,
    statement: &tokio_postgres::Statement,
) -> Result<(), tokio_postgres::Error> {
    let _: Vec<User> = client.query_as::<User>("SELECT id, name FROM users", &[]).await?;
    let _: User = client.query_one_as::<User>(statement, &[]).await?;
    let _: Option<User> = client
        .query_opt_as::<User>("SELECT id, name FROM users WHERE id = $1", &[&1i64])
        .await?;

    Ok(())
}

async fn assert_transaction_methods(
    transaction: &tokio_postgres::Transaction<'_>,
    statement: &tokio_postgres::Statement,
) -> Result<(), tokio_postgres::Error> {
    let _: Vec<User> = transaction
        .query_as::<User>("SELECT id, name FROM users", &[])
        .await?;
    let _: User = transaction.query_one_as::<User>(statement, &[]).await?;
    let _: Option<User> = transaction
        .query_opt_as::<User>("SELECT id, name FROM users WHERE id = $1", &[&1i64])
        .await?;

    Ok(())
}

async fn assert_generic_client_methods<C>(
    client: &C,
    statement: &tokio_postgres::Statement,
) -> Result<(), tokio_postgres::Error>
where
    C: GenericClient + Sync,
{
    let _: Vec<User> = client.query_as::<User>("SELECT id, name FROM users", &[]).await?;
    let _: User = client.query_one_as::<User>(statement, &[]).await?;
    let _: Option<User> = client
        .query_opt_as::<User>("SELECT id, name FROM users WHERE id = $1", &[&1i64])
        .await?;

    Ok(())
}

fn main() {
    assert_from_row::<User>();
    assert_from_row::<Renamed>();
    assert_from_row::<AuditLog>();
    assert_from_row::<Borrowed<'_>>();
    assert_generic::<i64>();
    assert_wrapped_generic::<()>();
}
