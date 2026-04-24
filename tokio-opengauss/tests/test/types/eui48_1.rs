use eui48_1::MacAddress;

use crate::types::test_type;

#[tokio::test]
async fn test_eui48_params() {
    test_type(
        "MACADDR",
        &[
            (
                Some(MacAddress::parse_str("12-34-56-AB-CD-EF").unwrap()),
                "'12-34-56-ab-cd-ef'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}
