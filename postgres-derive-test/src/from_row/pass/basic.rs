#![allow(non_upper_case_globals)]

use std::rc::Rc;

use postgres_types::{FromSql, ToSql};
use tokio_pg::{FromRow, GenericClient};
use r#type as tokio_pg;

#[derive(FromRow)]
struct User {
    id: i64,
    name: String,
}

#[derive(FromRow)]
struct Empty {}

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
struct MultipleBorrowed<'a, 'b> {
    name: &'a str,
    data: &'b [u8],
}

#[derive(FromRow)]
struct RowLifetimeCollision<'__tokio_postgres_row> {
    value: &'__tokio_postgres_row str,
}

#[derive(FromRow)]
struct MultipleLifetimeCollisions<'__tokio_postgres_row, '__tokio_postgres_row1> {
    name: &'__tokio_postgres_row str,
    data: &'__tokio_postgres_row1 [u8],
}

#[derive(FromRow)]
struct ConstNameCollision<const row: usize> {
    value: i32,
}

#[derive(FromRow)]
struct RowField {
    row: i32,
    value: i32,
}

#[derive(FromRow)]
struct Generic<T> {
    value: T,
}

struct Wrapper<T>(T);

impl<'a, T> tokio_pg::types::FromSql<'a> for Wrapper<T> {
    fn from_sql(
        _: &tokio_pg::types::Type,
        _: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        unimplemented!()
    }

    fn accepts(_: &tokio_pg::types::Type) -> bool {
        true
    }
}

#[derive(FromRow)]
struct WrappedGeneric<T> {
    value: Wrapper<T>,
}

#[derive(FromRow)]
struct IndependentLifetime<'a> {
    value: Wrapper<&'a ()>,
}

#[derive(FromRow)]
struct HigherRankedCollision {
    value: Wrapper<for<'__tokio_postgres_row> fn(&'__tokio_postgres_row str)>,
}

#[derive(FromRow)]
struct HigherRankedBoundCollision<T>
where
    T: for<'__tokio_postgres_row> Fn(&'__tokio_postgres_row str),
{
    value: Wrapper<T>,
}

#[derive(FromRow)]
struct DefaultGenerics<T = i32, const N: usize = 3> {
    value: Wrapper<[T; N]>,
}

trait HasValue {
    type Value;
}

#[derive(FromRow)]
struct AssociatedType<T: HasValue> {
    value: T::Value,
}

#[derive(Debug, FromRow, FromSql, ToSql)]
#[postgres(name = "shared_composite")]
struct SharedComposite {
    value: i32,
}

fn assert_from_row<'a, T>()
where
    T: FromRow<'a>,
{
}

fn assert_generic<'a, T>()
where
    T: tokio_pg::types::FromSql<'a>,
{
    assert_from_row::<Generic<T>>();
}

fn assert_wrapped_generic<T>() {
    assert_owned_from_row::<WrappedGeneric<T>>();
}

fn assert_associated_type<T>()
where
    T: HasValue,
    T::Value: for<'a> FromSql<'a>,
{
    assert_owned_from_row::<AssociatedType<T>>();
}

fn assert_owned_from_row<T>()
where
    T: for<'a> FromRow<'a>,
{
}

fn assert_multiple_borrowed<'a>() {
    assert_from_row::<MultipleBorrowed<'a, 'a>>();
}

async fn assert_client_methods(
    client: &tokio_pg::Client,
    statement: &tokio_pg::Statement,
) -> Result<(), tokio_pg::Error> {
    let _: Vec<User> = client
        .query_as::<User>("SELECT id, name FROM users", &[])
        .await?;
    let _: User = client.query_one_as::<User>(statement, &[]).await?;
    let _: Option<User> = client
        .query_opt_as::<User>("SELECT id, name FROM users WHERE id = $1", &[&1i64])
        .await?;

    Ok(())
}

async fn assert_transaction_methods(
    transaction: &tokio_pg::Transaction<'_>,
    statement: &tokio_pg::Statement,
) -> Result<(), tokio_pg::Error> {
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
    statement: &tokio_pg::Statement,
) -> Result<(), tokio_pg::Error>
where
    C: GenericClient,
{
    let _: Vec<User> = client
        .query_as::<User>("SELECT id, name FROM users", &[])
        .await?;
    let _: User = client.query_one_as::<User>(statement, &[]).await?;
    let _: Option<User> = client
        .query_opt_as::<User>("SELECT id, name FROM users WHERE id = $1", &[&1i64])
        .await?;

    let _: Vec<WrappedGeneric<Rc<()>>> = client.query_as("SELECT value", &[]).await?;
    let _: WrappedGeneric<Rc<()>> = client.query_one_as("SELECT value", &[]).await?;
    let _: Option<WrappedGeneric<Rc<()>>> = client.query_opt_as("SELECT value", &[]).await?;

    Ok(())
}

fn main() {
    assert_owned_from_row::<Empty>();
    assert_from_row::<User>();
    assert_from_row::<Renamed>();
    assert_from_row::<AuditLog>();
    assert_from_row::<Borrowed<'_>>();
    assert_from_row::<SharedComposite>();
    assert_from_row::<RowLifetimeCollision<'_>>();
    assert_from_row::<MultipleLifetimeCollisions<'_, '_>>();
    assert_owned_from_row::<ConstNameCollision<1>>();
    assert_owned_from_row::<RowField>();
    assert_owned_from_row::<HigherRankedCollision>();
    assert_owned_from_row::<HigherRankedBoundCollision<fn(&str)>>();
    assert_owned_from_row::<DefaultGenerics>();
    assert_owned_from_row::<DefaultGenerics<Rc<()>, 0>>();
    assert_generic::<i64>();
    assert_wrapped_generic::<()>();
    assert_owned_from_row::<IndependentLifetime<'static>>();
}
