use postgres::{Client, NoTls};
use postgres_types::{FromSql, ToSql};

#[test]
fn round_trip() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(transparent)]
    struct UserId(i32);

    assert_eq!(
        Client::connect("user=postgres host=localhost port=5433", NoTls)
            .unwrap()
            .query_one("SELECT $1::integer", &[&UserId(123)])
            .unwrap()
            .get::<_, UserId>(0),
        UserId(123)
    );
}

#[test]
fn struct_with_reference() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(transparent)]
    struct UserName<'a>(&'a str);

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let user_name = "tester";
    let row = conn
        .query_one("SELECT $1", &[&UserName(user_name)])
        .unwrap();
    let result: UserName<'_> = row.get(0);
    assert_eq!(user_name, result.0);
}

#[test]
fn nested_struct_with_reference() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(transparent)]
    struct Inner<'a>(&'a str);

    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(transparent)]
    struct UserName<'a>(#[postgres(borrow)] Inner<'a>);

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let user_name = "tester";
    let inner = Inner(user_name);
    let row = conn.query_one("SELECT $1", &[&UserName(inner)]).unwrap();
    let result: UserName<'_> = row.get(0);
    assert_eq!(user_name, result.0 .0);
}
