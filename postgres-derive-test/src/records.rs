use postgres::{Client, NoTls};
use postgres_types::{FromSql, ToSql, WrongType};
use std::error::Error;

#[test]
fn basic() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let expected = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let got = conn
        .query_one("SELECT ('foobar', 100, 15.50::double precision)", &[])
        .unwrap()
        .try_get::<_, InventoryItem>(0)
        .unwrap();

    assert_eq!(got, expected);
}

#[test]
fn field_count_mismatch() {
    #[derive(FromSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let err = conn
        .query_one("SELECT ('foobar', 100)", &[])
        .unwrap()
        .try_get::<_, InventoryItem>(0)
        .unwrap_err();
    err.source().unwrap().is::<WrongType>();

    let err = conn
        .query_one("SELECT ('foobar', 100, 15.50, 'extra')", &[])
        .unwrap()
        .try_get::<_, InventoryItem>(0)
        .unwrap_err();
    err.source().unwrap().is::<WrongType>();
}

#[test]
fn wrong_type() {
    #[derive(FromSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let err = conn
        .query_one("SELECT ('foobar', 'not_an_int', 15.50)", &[])
        .unwrap()
        .try_get::<_, InventoryItem>(0)
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());

    let err = conn
        .query_one("SELECT (123, 100, 15.50)", &[])
        .unwrap()
        .try_get::<_, InventoryItem>(0)
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn nested_structs() {
    #[derive(FromSql, Debug, PartialEq)]
    struct Address {
        street: String,
        city: Option<String>,
    }

    #[derive(FromSql, Debug, PartialEq)]
    struct Person {
        name: String,
        age: Option<i32>,
        address: Address,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let result: Person = conn
        .query_one(
            "SELECT ('John', 30, ROW('123 Main St', 'Springfield'))",
            &[],
        )
        .unwrap()
        .get(0);

    let expected = Person {
        name: "John".to_owned(),
        age: Some(30),
        address: Address {
            street: "123 Main St".to_owned(),
            city: Some("Springfield".to_owned()),
        },
    };

    assert_eq!(result, expected);
}

#[test]
fn domains() {
    #[derive(FromSql, Debug, PartialEq)]
    struct SpecialId(i32);

    #[derive(FromSql, Debug, PartialEq)]
    struct Person {
        name: String,
        age: Option<i32>,
        id: SpecialId,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute("CREATE DOMAIN pg_temp.\"special_id\" AS integer;", &[])
        .unwrap();

    let result: Person = conn
        .query_one("SELECT ('John', 30, 42::special_id)", &[])
        .unwrap()
        .get(0);

    let expected = Person {
        name: "John".to_owned(),
        age: Some(30),
        id: SpecialId(42),
    };

    assert_eq!(result, expected);
}

#[test]
fn enums() {
    #[derive(FromSql, Debug, PartialEq)]
    enum Employment {
        Salaried,
        Hourly,
        Unemployed,
    }

    #[derive(FromSql, Debug, PartialEq)]
    struct Person {
        name: String,
        age: Option<i32>,
        employment: Employment,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.execute(
        "CREATE TYPE pg_temp.employment AS ENUM ('Salaried', 'Hourly', 'Unemployed')",
        &[],
    )
    .unwrap();

    let result: Person = conn
        .query_one("SELECT ('John', 30, 'Hourly'::employment)", &[])
        .unwrap()
        .get(0);

    let expected = Person {
        name: "John".to_owned(),
        age: Some(30),
        employment: Employment::Hourly,
    };

    assert_eq!(result, expected);
}

#[test]
fn generics() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct GenericItem<T, U> {
        first: T,
        second: U,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();

    let expected = GenericItem {
        first: "test".to_owned(),
        second: 42,
    };

    let got = conn
        .query_one("SELECT ('test', 42)", &[])
        .unwrap()
        .try_get::<_, GenericItem<String, i32>>(0)
        .unwrap();

    assert_eq!(got, expected);
}
