use uuid_1::Uuid;

use crate::types::test_type;

#[tokio::test]
async fn test_uuid_params() {
    test_type(
        "UUID",
        &[
            (
                Some(Uuid::parse_str("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'",
            ),
            (None, "NULL"),
        ],
    )
    .await
}
