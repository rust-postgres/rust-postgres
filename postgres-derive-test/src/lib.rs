#![cfg(test)]

use postgres::Client;
use postgres_types::{FromSqlOwned, ToSql};
use std::fmt;

mod composites;
mod domains;
mod enums;
mod transparent;

pub fn test_type<T, S>(conn: &mut Client, sql_type: &str, checks: &[(T, S)])
where
    T: PartialEq + FromSqlOwned + ToSql + Sync,
    S: fmt::Display,
{
    for (val, repr) in checks.iter() {
        let stmt = conn
            .prepare(&format!("SELECT {}::{}", *repr, sql_type))
            .unwrap();
        let result = conn.query_one(&stmt, &[]).unwrap().get(0);
        assert_eq!(val, &result);

        let stmt = conn.prepare(&format!("SELECT $1::{sql_type}")).unwrap();
        let result = conn.query_one(&stmt, &[val]).unwrap().get(0);
        assert_eq!(val, &result);
    }
}

pub fn test_type_asymmetric<T, F, S, C>(
    conn: &mut Client,
    sql_type: &str,
    checks: &[(T, S)],
    cmp: C,
) where
    T: ToSql + Sync,
    F: FromSqlOwned,
    S: fmt::Display,
    C: Fn(&T, &F) -> bool,
{
    for (val, repr) in checks.iter() {
        let stmt = conn
            .prepare(&format!("SELECT {}::{}", *repr, sql_type))
            .unwrap();
        let result: F = conn.query_one(&stmt, &[]).unwrap().get(0);
        assert!(cmp(val, &result));

        let stmt = conn.prepare(&format!("SELECT $1::{sql_type}")).unwrap();
        let result: F = conn.query_one(&stmt, &[val]).unwrap().get(0);
        assert!(cmp(val, &result));
    }
}

#[test]
fn compile_fail() {
    trybuild::TestCases::new().compile_fail("src/compile-fail/*.rs");
}
