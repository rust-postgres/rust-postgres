use macaddr_1::{MacAddr6, MacAddr8};
use std::str::FromStr;

use crate::types::test_type;

#[tokio::test]
async fn test_macaddr6_params() {
    test_type(
        "MACADDR",
        &[
            (
                Some(MacAddr6::from_str("12-34-56-AB-CD-EF").unwrap()),
                "'12-34-56-ab-cd-ef'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}

#[tokio::test]
async fn test_macaddr8_params() {
    test_type(
        "MACADDR8",
        &[
            (
                Some(MacAddr8::from_str("12-34-56-78-90-AB-CD-EF").unwrap()),
                "'12-34-56-78-90-ab-cd-ef'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}
