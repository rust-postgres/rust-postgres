use r#type::FromRow;

#[derive(FromRow)]
struct User {
    #[postgres(name = 1)]
    id: i64,
}

fn main() {}
