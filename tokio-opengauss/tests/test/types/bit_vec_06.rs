use bit_vec_06::BitVec;

use crate::types::test_type;

#[tokio::test]
async fn test_bit_params() {
    let mut bv = BitVec::from_bytes(&[0b0110_1001, 0b0000_0111]);
    bv.pop();
    bv.pop();
    test_type(
        "BIT(14)",
        &[(Some(bv), "B'01101001000001'"), (None, "NULL")],
    )
    .await
}

#[tokio::test]
async fn test_varbit_params() {
    let mut bv = BitVec::from_bytes(&[0b0110_1001, 0b0000_0111]);
    bv.pop();
    bv.pop();
    test_type(
        "VARBIT",
        &[
            (Some(bv), "B'01101001000001'"),
            (Some(BitVec::from_bytes(&[])), "B''"),
            (None, "NULL"),
        ],
    )
    .await
}
