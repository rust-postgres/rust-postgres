use jiff_01::{
    civil::{Date as JiffDate, DateTime, Time},
    Timestamp as JiffTimestamp,
};
use std::fmt;
use tokio_postgres::{
    types::{Date, FromSqlOwned, Timestamp},
    Client,
};

use crate::connect;
use crate::types::test_type;

#[tokio::test]
async fn test_datetime_params() {
    fn make_check(s: &str) -> (Option<DateTime>, &str) {
        (Some(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_datetime_params() {
    fn make_check(s: &str) -> (Timestamp<DateTime>, &str) {
        (Timestamp::Value(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_timestamp_params() {
    fn make_check(s: &str) -> (Option<JiffTimestamp>, &str) {
        (Some(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000Z'"),
            make_check("'1965-09-25 11:19:33.100314000Z'"),
            make_check("'2010-02-09 23:11:45.120200000Z'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_timestamp_params() {
    fn make_check(s: &str) -> (Timestamp<JiffTimestamp>, &str) {
        (Timestamp::Value(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000Z'"),
            make_check("'1965-09-25 11:19:33.100314000Z'"),
            make_check("'2010-02-09 23:11:45.120200000Z'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_date_params() {
    fn make_check(s: &str) -> (Option<JiffDate>, &str) {
        (Some(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_date_params() {
    fn make_check(s: &str) -> (Date<JiffDate>, &str) {
        (Date::Value(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (Date::PosInfinity, "'infinity'"),
            (Date::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_time_params() {
    fn make_check(s: &str) -> (Option<Time>, &str) {
        (Some(s.trim_matches('\'').parse().unwrap()), s)
    }
    test_type(
        "TIME",
        &[
            make_check("'00:00:00.010000000'"),
            make_check("'11:19:33.100314000'"),
            make_check("'23:11:45.120200000'"),
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_special_params_without_wrapper() {
    async fn assert_overflows<T>(client: &mut Client, val: &str, sql_type: &str)
    where
        T: FromSqlOwned + fmt::Debug,
    {
        let err = client
            .query_one(&*format!("SELECT {val}::{sql_type}"), &[])
            .await
            .unwrap()
            .try_get::<_, T>(0)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing column 0: value too large to decode"
        );

        let err = client
            .query_one(&*format!("SELECT {val}::{sql_type}"), &[])
            .await
            .unwrap()
            .try_get::<_, T>(0)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "error deserializing column 0: value too large to decode"
        );
    }

    let mut client = connect("user=postgres").await;

    assert_overflows::<DateTime>(&mut client, "'-infinity'", "timestamp").await;
    assert_overflows::<DateTime>(&mut client, "'infinity'", "timestamp").await;
    assert_overflows::<JiffTimestamp>(&mut client, "'-infinity'", "timestamptz").await;
    assert_overflows::<JiffTimestamp>(&mut client, "'infinity'", "timestamptz").await;
    assert_overflows::<JiffDate>(&mut client, "'-infinity'", "date").await;
    assert_overflows::<JiffDate>(&mut client, "'infinity'", "date").await;
}
