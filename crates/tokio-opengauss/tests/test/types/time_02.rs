use time_02::{OffsetDateTime, PrimitiveDateTime};
use tokio_opengauss::types::{Date, Timestamp};

use crate::types::test_type;

// time 0.2 does not [yet?] support parsing fractional seconds
// https://github.com/time-rs/time/issues/226

#[tokio::test]
async fn test_primitive_date_time_params() {
    fn make_check(time: &str) -> (Option<PrimitiveDateTime>, &str) {
        (
            Some(PrimitiveDateTime::parse(time, "'%Y-%m-%d %H:%M:%S'").unwrap()),
            time,
        )
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
        (
            Timestamp::Value(PrimitiveDateTime::parse(time, "'%Y-%m-%d %H:%M:%S'").unwrap()),
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
        (
            Some(OffsetDateTime::parse(time, "'%Y-%m-%d %H:%M:%S %z'").unwrap()),
            time,
        )
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
        (
            Timestamp::Value(OffsetDateTime::parse(time, "'%Y-%m-%d %H:%M:%S %z'").unwrap()),
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
    fn make_check(time: &str) -> (Option<time_02::Date>, &str) {
        (
            Some(time_02::Date::parse(time, "'%Y-%m-%d'").unwrap()),
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
    fn make_check(date: &str) -> (Date<time_02::Date>, &str) {
        (
            Date::Value(time_02::Date::parse(date, "'%Y-%m-%d'").unwrap()),
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
    fn make_check(time: &str) -> (Option<time_02::Time>, &str) {
        (
            Some(time_02::Time::parse(time, "'%H:%M:%S'").unwrap()),
            time,
        )
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
