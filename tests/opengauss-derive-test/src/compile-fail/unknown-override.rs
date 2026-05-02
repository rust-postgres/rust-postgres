use opengauss_types::{FromSql, ToSql};

#[derive(FromSql)]
#[postgres(foo = "bar")]
struct Foo {
    a: i32,
}

#[derive(ToSql)]
#[postgres(foo = "bar")]
struct Bar {
    a: i32,
}

fn main() {}
