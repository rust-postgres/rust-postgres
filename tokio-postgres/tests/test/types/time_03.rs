use std::fmt;

use postgres_types::FromSqlOwned;
use time_03::{format_description, OffsetDateTime, PrimitiveDateTime};
use tokio_postgres::{
    types::{Date, Timestamp},
    Client,
};

use crate::types::test_type;

// time 0.2 does not [yet?] support parsing fractional seconds
// https://github.com/time-rs/time/issues/226

#[tokio::test]
async fn test_primitive_date_time_params() {
    fn make_check(time: &str) -> (Option<PrimitiveDateTime>, &str) {
        let format =
            format_description::parse("'[year]-[month]-[day] [hour]:[minute]:[second]'").unwrap();
        (Some(PrimitiveDateTime::parse(time, &format).unwrap()), time)
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00'"), // .010000000
            make_check("'1965-09-25 11:19:33'"), // .100314000
            make_check("'2010-02-09 23:11:45'"), // .120200000
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_primitive_date_time_params() {
    fn make_check(time: &str) -> (Timestamp<PrimitiveDateTime>, &str) {
        let format =
            format_description::parse("'[year]-[month]-[day] [hour]:[minute]:[second]'").unwrap();
        (
            Timestamp::Value(PrimitiveDateTime::parse(time, &format).unwrap()),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00'"), // .010000000
            make_check("'1965-09-25 11:19:33'"), // .100314000
            make_check("'2010-02-09 23:11:45'"), // .120200000
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_offset_date_time_params() {
    fn make_check(time: &str) -> (Option<OffsetDateTime>, &str) {
        let format =
            format_description::parse("'[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]'").unwrap();
        (Some(OffsetDateTime::parse(time, &format).unwrap()), time)
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00 +0000'"), // .010000000
            make_check("'1965-09-25 11:19:33 +0000'"), // .100314000
            make_check("'2010-02-09 23:11:45 +0000'"), // .120200000
            (None, "NULL"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_with_special_offset_date_time_params() {
    fn make_check(time: &str) -> (Timestamp<OffsetDateTime>, &str) {
        let format =
            format_description::parse("'[year]-[month]-[day] [hour]:[minute]:[second] [offset_hour sign:mandatory][offset_minute]'").unwrap();
        (
            Timestamp::Value(OffsetDateTime::parse(time, &format).unwrap()),
            time,
        )
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00 +0000'"), // .010000000
            make_check("'1965-09-25 11:19:33 +0000'"), // .100314000
            make_check("'2010-02-09 23:11:45 +0000'"), // .120200000
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_date_params() {
    fn make_check(date: &str) -> (Option<time_03::Date>, &str) {
        let format = format_description::parse("'[year]-[month]-[day]'").unwrap();
        (Some(time_03::Date::parse(date, &format).unwrap()), date)
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
    fn make_check(date: &str) -> (Date<time_03::Date>, &str) {
        let format = format_description::parse("'[year]-[month]-[day]'").unwrap();
        (
            Date::Value(time_03::Date::parse(date, &format).unwrap()),
            date,
        )
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
    fn make_check(time: &str) -> (Option<time_03::Time>, &str) {
        let format = format_description::parse("'[hour]:[minute]:[second]'").unwrap();
        (Some(time_03::Time::parse(time, &format).unwrap()), time)
    }
    test_type(
        "TIME",
        &[
            make_check("'00:00:00'"), // .010000000
            make_check("'11:19:33'"), // .100314000
            make_check("'23:11:45'"), // .120200000
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
    }

    let mut client = crate::connect("user=postgres").await;

    assert_overflows::<OffsetDateTime>(&mut client, "'-infinity'", "timestamptz").await;
    assert_overflows::<OffsetDateTime>(&mut client, "'infinity'", "timestamptz").await;

    assert_overflows::<PrimitiveDateTime>(&mut client, "'-infinity'", "timestamp").await;
    assert_overflows::<PrimitiveDateTime>(&mut client, "'infinity'", "timestamp").await;

    assert_overflows::<time_03::Date>(&mut client, "'-infinity'", "date").await;
    assert_overflows::<time_03::Date>(&mut client, "'infinity'", "date").await;
}
