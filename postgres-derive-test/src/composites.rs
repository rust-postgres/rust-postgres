use crate::{test_type, test_type_asymmetric};
use postgres::{Client, NoTls};
use postgres_types::{FromSql, ToSql, WrongType};
use std::error::Error;

#[test]
fn defaults() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.\"InventoryItem\" AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let item_null = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: None,
    };

    test_type(
        &mut conn,
        "\"InventoryItem\"",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
    );
}

#[test]
fn nested() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryBundle {
        a: InventoryItem,
        b: InventoryItem,
    }

    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.\"InventoryItem\" AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.\"InventoryBundle\" AS (
            a pg_temp.\"InventoryItem\",
            b pg_temp.\"InventoryItem\"
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let item_null = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: None,
    };

    let bundle = InventoryBundle {
        a: item,
        b: item_null,
    };

    test_type(
        &mut conn,
        "\"InventoryBundle\"",
        &[(
            bundle,
            "ROW(ROW('foobar', 100, 15.50), ROW('foobar', 100, NULL))",
        )],
    );
}

#[test]
fn name_overrides() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        #[postgres(name = "name")]
        _name: String,
        #[postgres(name = "supplier_id")]
        _supplier_id: i32,
        #[postgres(name = "price")]
        _price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        _name: "foobar".to_owned(),
        _supplier_id: 100,
        _price: Some(15.50),
    };

    let item_null = InventoryItem {
        _name: "foobar".to_owned(),
        _supplier_id: 100,
        _price: None,
    };

    test_type(
        &mut conn,
        "inventory_item",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
    );
}

#[test]
fn rename_all_overrides() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item", rename_all = "SCREAMING_SNAKE_CASE")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        #[postgres(name = "Price")]
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            \"NAME\" TEXT,
            \"SUPPLIER_ID\" INT,
            \"Price\" DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let item_null = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: None,
    };

    test_type(
        &mut conn,
        "inventory_item",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
    );
}

#[test]
fn wrong_name() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn extra_field() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: Option<f64>,
        foo: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: Some(15.50),
        foo: 0,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn missing_field() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn wrong_type() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        name: String,
        supplier_id: i32,
        price: i32,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItem {
        name: "foobar".to_owned(),
        supplier_id: 100,
        price: 0,
    };

    let err = conn
        .execute("SELECT $1::inventory_item", &[&item])
        .unwrap_err();
    assert!(err.source().unwrap().is::<WrongType>());
}

#[test]
fn raw_ident_field() {
    #[derive(FromSql, ToSql, Debug, PartialEq)]
    #[postgres(name = "inventory_item")]
    struct InventoryItem {
        r#type: String,
    }

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.inventory_item AS (
            type TEXT
        )",
    )
    .unwrap();

    let item = InventoryItem {
        r#type: "foo".to_owned(),
    };

    test_type(&mut conn, "inventory_item", &[(item, "ROW('foo')")]);
}

#[test]
fn generics() {
    #[derive(FromSql, Debug, PartialEq)]
    struct InventoryItem<T: Clone, U>
    where
        U: Clone,
    {
        name: String,
        supplier_id: T,
        price: Option<U>,
    }

    // doesn't make sense to implement derived FromSql on a type with borrows
    #[derive(ToSql, Debug, PartialEq)]
    #[postgres(name = "InventoryItem")]
    struct InventoryItemRef<'a, T: 'a + Clone, U>
    where
        U: 'a + Clone,
    {
        name: &'a str,
        supplier_id: &'a T,
        price: Option<&'a U>,
    }

    const NAME: &str = "foobar";
    const SUPPLIER_ID: i32 = 100;
    const PRICE: f64 = 15.50;

    let mut conn = Client::connect("user=postgres host=localhost port=5433", NoTls).unwrap();
    conn.batch_execute(
        "CREATE TYPE pg_temp.\"InventoryItem\" AS (
            name TEXT,
            supplier_id INT,
            price DOUBLE PRECISION
        );",
    )
    .unwrap();

    let item = InventoryItemRef {
        name: NAME,
        supplier_id: &SUPPLIER_ID,
        price: Some(&PRICE),
    };

    let item_null = InventoryItemRef {
        name: NAME,
        supplier_id: &SUPPLIER_ID,
        price: None,
    };

    test_type_asymmetric(
        &mut conn,
        "\"InventoryItem\"",
        &[
            (item, "ROW('foobar', 100, 15.50)"),
            (item_null, "ROW('foobar', 100, NULL)"),
        ],
        |t: &InventoryItemRef<i32, f64>, f: &InventoryItem<i32, f64>| {
            t.name == f.name.as_str()
                && t.supplier_id == &f.supplier_id
                && t.price == f.price.as_ref()
        },
    );
}
