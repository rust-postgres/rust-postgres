use super::connect;
use tokio_postgres::FromRow;

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

    let row = client
        .query_one(
            "SELECT 'shown'::TEXT AS \"display-name\", 'kind'::TEXT AS \"type\", NULL::TEXT AS \"nickName\", true AS extra",
            &[],
        )
        .await
        .unwrap();
    let value = UnusualNames::from_row(&row).unwrap();

    assert_eq!(
        value,
        UnusualNames {
            display_name: "shown".to_string(),
            r#type: "kind".to_string(),
            nick_name: None,
        }
    );

    let row = client
        .query_one(
            "SELECT 1::INT AS \"display-name\", 'kind'::TEXT AS \"type\", NULL::TEXT AS \"nickName\"",
            &[],
        )
        .await
        .unwrap();
    let error = UnusualNames::from_row(&row).unwrap_err();
    assert_eq!(error.to_string(), "error deserializing column 0");

    let row = client
        .query_one(
            "SELECT 'shown'::TEXT AS \"display-name\", 'kind'::TEXT AS \"type\"",
            &[],
        )
        .await
        .unwrap();
    let error = UnusualNames::from_row(&row).unwrap_err();
    assert_eq!(error.to_string(), "invalid column `nickName`");
}
