use r#type::FromRow;

#[derive(FromRow)]
#[postgres(transparent)]
struct TransparentContainer {
    value: i64,
}

#[derive(FromRow)]
#[postgres(allow_mismatch)]
struct MismatchedContainer {
    value: i64,
}

#[derive(FromRow)]
struct TransparentField {
    #[postgres(transparent)]
    value: i64,
}

#[derive(FromRow)]
struct MismatchedField {
    #[postgres(allow_mismatch)]
    value: i64,
}

fn main() {}
