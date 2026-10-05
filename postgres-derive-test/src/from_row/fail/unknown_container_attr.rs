use r#type::FromRow;

#[derive(FromRow)]
#[postgres(foo = "bar")]
struct User {
    id: i64,
}

fn main() {}
