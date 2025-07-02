use chrono_04::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use std::fmt;
use tokio_postgres::types::{Date, FromSqlOwned, Timestamp};
use tokio_postgres::Client;

use crate::connect;
use crate::types::test_type;

#[tokio::test]
async fn test_naive_date_time_params() {
    fn make_check(time: &str) -> (Option<NaiveDateTime>, &str) {
        (
            Some(NaiveDateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap()),
            time,
        )
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
async fn test_with_special_naive_date_time_params() {
    fn make_check(time: &str) -> (Timestamp<NaiveDateTime>, &str) {
        (
            Timestamp::Value(
                NaiveDateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap(),
            ),
            time,
        )
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
async fn test_date_time_params() {
    fn make_check(time: &str) -> (Option<DateTime<Utc>>, &str) {
        (
            Some(
                DateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f%#z'")
                    .unwrap()
                    .to_utc(),
            ),
            time,
        )
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
async fn test_with_special_date_time_params() {
    fn make_check(time: &str) -> (Timestamp<DateTime<Utc>>, &str) {
        (
            Timestamp::Value(
                DateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f%#z'")
                    .unwrap()
                    .to_utc(),
            ),
            time,
        )
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
    fn make_check(time: &str) -> (Option<NaiveDate>, &str) {
        (
            Some(NaiveDate::parse_from_str(time, "'%Y-%m-%d'").unwrap()),
            time,
        )
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
    fn make_check(date: &str) -> (Date<NaiveDate>, &str) {
        (
            Date::Value(NaiveDate::parse_from_str(date, "'%Y-%m-%d'").unwrap()),
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
    fn make_check(time: &str) -> (Option<NaiveTime>, &str) {
        (
            Some(NaiveTime::parse_from_str(time, "'%H:%M:%S.%f'").unwrap()),
            time,
        )
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
    }

    let mut client = connect("user=postgres").await;

    assert_overflows::<DateTime<Utc>>(&mut client, "'-infinity'", "timestamptz").await;
    assert_overflows::<DateTime<Utc>>(&mut client, "'infinity'", "timestamptz").await;

    assert_overflows::<NaiveDateTime>(&mut client, "'-infinity'", "timestamp").await;
    assert_overflows::<NaiveDateTime>(&mut client, "'infinity'", "timestamp").await;

    assert_overflows::<NaiveDate>(&mut client, "'-infinity'", "date").await;
    assert_overflows::<NaiveDate>(&mut client, "'infinity'", "date").await;
}
