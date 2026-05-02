use opengauss_types::{FromSql, ToSql};

#[derive(ToSql)]
struct ToSqlUnit;

#[derive(FromSql)]
struct FromSqlUnit;

#[derive(ToSql)]
struct ToSqlTuple(i32, i32);

#[derive(FromSql)]
struct FromSqlTuple(i32, i32);

#[derive(ToSql)]
enum ToSqlEnum {
    Foo(i32),
}

#[derive(FromSql)]
enum FromSqlEnum {
    Foo(i32),
}

fn main() {}
