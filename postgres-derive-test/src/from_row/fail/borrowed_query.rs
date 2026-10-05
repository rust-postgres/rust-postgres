use r#type::{Client, FromRow};

#[derive(FromRow)]
struct User<'a> {
    name: &'a str,
}

fn load(client: &Client) {
    let _future = client.query_one_as::<User<'_>>("SELECT name FROM users", &[]);
}

fn main() {}
