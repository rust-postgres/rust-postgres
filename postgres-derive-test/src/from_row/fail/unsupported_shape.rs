use r#type::FromRow;

#[derive(FromRow)]
struct User(i64);

#[derive(FromRow)]
struct Unit;

#[derive(FromRow)]
enum Enumeration {
    Value { id: i32 },
}

#[derive(FromRow)]
union Union {
    id: i32,
}

fn main() {}
