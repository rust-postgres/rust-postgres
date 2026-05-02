use opengauss::{Client, NoTls};
use opengauss_types::{FromSql, ToSql};

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
