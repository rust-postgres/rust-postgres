#![cfg(feature = "derive")]

#[test]
fn from_row_derive_passes() {
    let test = trybuild::TestCases::new();
    test.pass("tests/derive/pass/*.rs");
    test.compile_fail("tests/derive/fail/*.rs");
}
