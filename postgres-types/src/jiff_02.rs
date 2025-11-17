use bytes::BytesMut;
use jiff_02::{
    civil::{Date, DateTime, Time},
    Span, SpanRound, Timestamp, Unit,
};
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

const fn base() -> DateTime {
    DateTime::constant(2000, 1, 1, 0, 0, 0, 0)
}

/// The number of seconds from the Unix epoch to 2000-01-01 00:00:00 UTC.
const PG_EPOCH: i64 = 946684800;

fn base_ts() -> Timestamp {
    Timestamp::new(PG_EPOCH, 0).unwrap()
}

fn round_us<'a>() -> SpanRound<'a> {
    SpanRound::new().largest(Unit::Microsecond)
}

fn decode_err<E>(_e: E) -> Box<dyn Error + Sync + Send>
where
    E: Error,
{
    "value too large to decode".into()
}

fn transmit_err<E>(_e: E) -> Box<dyn Error + Sync + Send>
where
    E: Error,
{
    "value too large to transmit".into()
}

impl<'a> FromSql<'a> for DateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<DateTime, Box<dyn Error + Sync + Send>> {
        let v = types::timestamp_from_sql(raw)?;
        Span::new()
            .try_microseconds(v)
            .and_then(|s| base().checked_add(s))
            .map_err(decode_err)
    }

    accepts!(TIMESTAMP);
}

impl ToSql for DateTime {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let v = self
            .since(base())
            .and_then(|s| s.round(round_us().relative(base())))
            .map_err(transmit_err)?
            .get_microseconds();
        types::timestamp_to_sql(v, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Timestamp {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Timestamp, Box<dyn Error + Sync + Send>> {
        let v = types::timestamp_from_sql(raw)?;
        Span::new()
            .try_microseconds(v)
            .and_then(|s| base_ts().checked_add(s))
            .map_err(decode_err)
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for Timestamp {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let v = self
            .since(base_ts())
            .and_then(|s| s.round(round_us()))
            .map_err(transmit_err)?
            .get_microseconds();
        types::timestamp_to_sql(v, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Date {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Date, Box<dyn Error + Sync + Send>> {
        let v = types::date_from_sql(raw)?;
        Span::new()
            .try_days(v)
            .and_then(|s| base().date().checked_add(s))
            .map_err(decode_err)
    }
    accepts!(DATE);
}

impl ToSql for Date {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let v = self.since(base().date()).map_err(transmit_err)?.get_days();
        types::date_to_sql(v, w);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Time {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Time, Box<dyn Error + Sync + Send>> {
        let v = types::time_from_sql(raw)?;
        Span::new()
            .try_microseconds(v)
            .and_then(|s| Time::midnight().checked_add(s))
            .map_err(decode_err)
    }

    accepts!(TIME);
}

impl ToSql for Time {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let v = self
            .since(Time::midnight())
            .and_then(|s| s.round(round_us()))
            .map_err(transmit_err)?
            .get_microseconds();
        types::time_to_sql(v, w);
        Ok(IsNull::No)
    }

    accepts!(TIME);
    to_sql_checked!();
}
