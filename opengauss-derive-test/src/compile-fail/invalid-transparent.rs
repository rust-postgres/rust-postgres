use opengauss_types::{FromSql, ToSql};

#[derive(ToSql, Debug)]
#[postgres(transparent)]
struct ToSqlTransparentStruct {
    a: i32
}

#[derive(FromSql, Debug)]
#[postgres(transparent)]
struct FromSqlTransparentStruct {
    a: i32
}

#[derive(ToSql, Debug)]
#[postgres(transparent)]
enum ToSqlTransparentEnum {
    Foo
}

#[derive(FromSql, Debug)]
#[postgres(transparent)]
enum FromSqlTransparentEnum {
    Foo
}

#[derive(ToSql, Debug)]
#[postgres(transparent)]
struct ToSqlTransparentTwoFieldTupleStruct(i32, i32);

#[derive(FromSql, Debug)]
#[postgres(transparent)]
struct FromSqlTransparentTwoFieldTupleStruct(i32, i32);

fn main() {}
