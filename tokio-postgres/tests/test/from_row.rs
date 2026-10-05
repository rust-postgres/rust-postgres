use super::connect;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Error, FromRow, GenericClient, Row};

#[derive(Debug, PartialEq)]
struct User {
    id: i32,
    name: String,
}

impl<'a> FromRow<'a> for User {
    fn from_row(row: &'a Row) -> Result<Self, Error> {
        let id = row.try_get("id")?;
        let name = row.try_get("name")?;

        Ok(User { id, name })
    }
}

async fn query_user<C>(client: &C, id: i32) -> Result<User, Error>
where
    C: GenericClient,
{
    client
        .query_one_as(
            "SELECT $1::INT AS id, ('user-' || ($1::INT)::TEXT) AS name",
            &[&id],
        )
        .await
}

#[cfg(feature = "derive")]
#[tokio::test]
async fn derives_borrowed_fields_from_a_live_row() {
    #[derive(Debug, FromRow, PartialEq)]
    struct Borrowed<'a> {
        label: &'a str,
        payload: &'a [u8],
    }

    let client = connect("user=postgres").await;
    let row = client
        .query_one(
            "SELECT 'borrowed'::TEXT AS label, decode('000102ff', 'hex') AS payload",
            &[],
        )
        .await
        .unwrap();

    let value = Borrowed::from_row(&row).unwrap();

    assert_eq!(
        value,
        Borrowed {
            label: "borrowed",
            payload: &[0, 1, 2, 255],
        }
    );
}

#[tokio::test]
async fn client_maps_rows_and_preserves_query_errors() {
    const ONE: &str = "SELECT 1::INT AS id, 'alice'::TEXT AS name";
    const NONE: &str = "SELECT 1::INT AS id, 'alice'::TEXT AS name WHERE FALSE";
    const MANY: &str = "SELECT id, name FROM (VALUES (1::INT, 'alice'::TEXT), (2, 'bob')) AS users(id, name) ORDER BY id";
    const MISSING_COLUMN: &str = "SELECT 1::INT AS id";
    const WRONG_TYPE: &str = "SELECT 'not an integer'::TEXT AS id, 'alice'::TEXT AS name";

    let client = connect("user=postgres").await;

    let users = client.query_as::<User>(MANY, &[]).await.unwrap();
    assert_eq!(
        users,
        vec![
            User {
                id: 1,
                name: "alice".to_string(),
            },
            User {
                id: 2,
                name: "bob".to_string(),
            },
        ]
    );

    let statement = client.prepare(ONE).await.unwrap();
    let user = client.query_one_as::<User>(&statement, &[]).await.unwrap();
    assert_eq!(
        user,
        User {
            id: 1,
            name: "alice".to_string(),
        }
    );

    let user = client.query_opt_as::<User>(ONE, &[]).await.unwrap();
    assert_eq!(
        user,
        Some(User {
            id: 1,
            name: "alice".to_string(),
        })
    );
    let user = client.query_opt_as::<User>(NONE, &[]).await.unwrap();
    assert_eq!(user, None);
    let users = client.query_as::<User>(NONE, &[]).await.unwrap();
    assert!(users.is_empty());

    let error = client.query_one_as::<User>(NONE, &[]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "query returned an unexpected number of rows"
    );
    let error = client.query_one_as::<User>(MANY, &[]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "query returned an unexpected number of rows"
    );
    let error = client.query_opt_as::<User>(MANY, &[]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "query returned an unexpected number of rows"
    );

    let error = client.query_as::<User>(WRONG_TYPE, &[]).await.unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");
    let error = client
        .query_one_as::<User>(WRONG_TYPE, &[])
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");
    let error = client
        .query_opt_as::<User>(WRONG_TYPE, &[])
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");

    let error = client
        .query_one_as::<User>(MISSING_COLUMN, &[])
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "invalid column `name`");

    let error = client
        .query_one_as::<User>("SELECT 1 / 0 AS id", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code(), Some(&SqlState::DIVISION_BY_ZERO));
}

#[tokio::test]
async fn mapping_stops_at_errors_and_follows_row_count_checks() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    static MAPPINGS: AtomicUsize = AtomicUsize::new(0);

    #[derive(Debug)]
    struct Mapped;

    impl<'a> FromRow<'a> for Mapped {
        fn from_row(row: &'a Row) -> Result<Self, Error> {
            MAPPINGS.fetch_add(1, Ordering::Relaxed);
            let _: i32 = row.try_get(0)?;

            Ok(Mapped)
        }
    }

    const QUERY: &str = "SELECT value FROM (VALUES (1, 1), (2, NULL), (3, 3)) AS rows(position, value) ORDER BY position";
    let client = connect("user=postgres").await;

    let error = client.query_as::<Mapped>(QUERY, &[]).await.unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");
    let mappings = MAPPINGS.load(Ordering::Relaxed);
    assert_eq!(mappings, 2);

    MAPPINGS.store(0, Ordering::Relaxed);
    let error = client.query_one_as::<Mapped>(QUERY, &[]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "query returned an unexpected number of rows"
    );
    let error = client.query_opt_as::<Mapped>(QUERY, &[]).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "query returned an unexpected number of rows"
    );
    let mappings = MAPPINGS.load(Ordering::Relaxed);
    assert_eq!(mappings, 0);
}

#[tokio::test]
async fn transaction_and_generic_client_map_rows() {
    let mut client = connect("user=postgres").await;

    let user = query_user(&client, 3).await.unwrap();
    assert_eq!(
        user,
        User {
            id: 3,
            name: "user-3".to_string(),
        }
    );

    let transaction = client.transaction().await.unwrap();
    let users = transaction
        .query_as::<User>(
            "SELECT id, ('user-' || id::TEXT) AS name FROM generate_series(4, 5) AS users(id) ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        users,
        vec![
            User {
                id: 4,
                name: "user-4".to_string(),
            },
            User {
                id: 5,
                name: "user-5".to_string(),
            },
        ]
    );

    let user = transaction
        .query_one_as::<User>("SELECT 6::INT AS id, 'user-6'::TEXT AS name", &[])
        .await
        .unwrap();
    assert_eq!(
        user,
        User {
            id: 6,
            name: "user-6".to_string(),
        }
    );

    let user = transaction
        .query_opt_as::<User>(
            "SELECT 7::INT AS id, 'user-7'::TEXT AS name WHERE FALSE",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(user, None);

    let user = query_user(&transaction, 8).await.unwrap();
    assert_eq!(
        user,
        User {
            id: 8,
            name: "user-8".to_string(),
        }
    );

    transaction.rollback().await.unwrap();
}

#[cfg(feature = "derive")]
#[tokio::test]
async fn derives_named_fields_and_preserves_column_decoding() {
    #[derive(Debug, FromRow, PartialEq)]
    #[postgres(rename_all = "camelCase")]
    struct UnusualNames {
        #[postgres(name = "display-name")]
        display_name: String,
        r#type: String,
        nick_name: Option<String>,
    }

    let client = connect("user=postgres").await;

    let value = client
        .query_one_as::<UnusualNames>(
            "SELECT 'shown'::TEXT AS \"display-name\", 'kind'::TEXT AS \"type\", NULL::TEXT AS \"nickName\", true AS extra",
            &[],
        )
        .await
        .unwrap();

    assert_eq!(
        value,
        UnusualNames {
            display_name: "shown".to_string(),
            r#type: "kind".to_string(),
            nick_name: None,
        }
    );

    let error = client
        .query_one_as::<UnusualNames>(
            "SELECT 1::INT AS \"display-name\", 'kind'::TEXT AS \"type\", NULL::TEXT AS \"nickName\"",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");

    let error = client
        .query_one_as::<UnusualNames>(
            "SELECT 'shown'::TEXT AS \"display-name\", 'kind'::TEXT AS \"type\"",
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "invalid column `nickName`");
}

#[cfg(feature = "derive")]
#[tokio::test]
async fn honors_every_rename_all_rule() {
    macro_rules! rename_case {
        ($name:ident, $rule:literal) => {
            #[allow(non_snake_case)]
            #[derive(FromRow)]
            #[postgres(rename_all = $rule)]
            struct $name {
                sampleField: i32,
            }
        };
    }

    rename_case!(Lowercase, "lowercase");
    rename_case!(Uppercase, "UPPERCASE");
    rename_case!(PascalCase, "PascalCase");
    rename_case!(CamelCase, "camelCase");
    rename_case!(SnakeCase, "snake_case");
    rename_case!(ScreamingSnakeCase, "SCREAMING_SNAKE_CASE");
    rename_case!(KebabCase, "kebab-case");
    rename_case!(ScreamingKebabCase, "SCREAMING-KEBAB-CASE");
    rename_case!(TrainCase, "Train-Case");

    let client = connect("user=postgres").await;
    let row = client
        .query_one(
            r#"
                SELECT
                    1::INT AS "samplefield",
                    2::INT AS "SAMPLEFIELD",
                    3::INT AS "SampleField",
                    4::INT AS "sampleField",
                    5::INT AS "sample_field",
                    6::INT AS "SAMPLE_FIELD",
                    7::INT AS "sample-field",
                    8::INT AS "SAMPLE-FIELD",
                    9::INT AS "Sample-Field"
            "#,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(Lowercase::from_row(&row).unwrap().sampleField, 1);
    assert_eq!(Uppercase::from_row(&row).unwrap().sampleField, 2);
    assert_eq!(PascalCase::from_row(&row).unwrap().sampleField, 3);
    assert_eq!(CamelCase::from_row(&row).unwrap().sampleField, 4);
    assert_eq!(SnakeCase::from_row(&row).unwrap().sampleField, 5);
    assert_eq!(ScreamingSnakeCase::from_row(&row).unwrap().sampleField, 6);
    assert_eq!(KebabCase::from_row(&row).unwrap().sampleField, 7);
    assert_eq!(ScreamingKebabCase::from_row(&row).unwrap().sampleField, 8);
    assert_eq!(TrainCase::from_row(&row).unwrap().sampleField, 9);
}
