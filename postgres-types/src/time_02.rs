use bytes::BytesMut;
use postgres_protocol::types;
use std::error::Error;
use time_02::{Date, Duration, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset, date, time};

use crate::{FromSql, IsNull, ToSql, Type};

#[rustfmt::skip]
const fn base() -> PrimitiveDateTime {
    PrimitiveDateTime::new(date!(2000-01-01), time!(00:00:00))
}

// `time` 0.2 represents years in the range -100_000..=100_000 and its `Add`
// implementations panic (rather than returning an error) when the result falls
// outside that range. Unlike `time` 0.3 it has no `checked_add`, so the
// resulting Julian day is validated against the representable range before the
// add is performed.
fn date_in_range(julian_day: i64) -> bool {
    let min = Date::try_from_ymd(-100_000, 1, 1)
        .expect("year is in range")
        .julian_day();
    let max = Date::try_from_ymd(100_000, 12, 31)
        .expect("year is in range")
        .julian_day();
    (min..=max).contains(&julian_day)
}

impl<'a> FromSql<'a> for PrimitiveDateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<PrimitiveDateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        // adding the sub-day remainder can shift the date by at most one day, so
        // a one-day margin guarantees the add below cannot overflow the range.
        let julian_day = base().date().julian_day() + Duration::microseconds(t).whole_days();
        if !date_in_range(julian_day - 1) || !date_in_range(julian_day + 1) {
            return Err("value too large to decode".into());
        }
        Ok(base() + Duration::microseconds(t))
    }

    accepts!(TIMESTAMP);
}

impl ToSql for PrimitiveDateTime {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let time = match i64::try_from((*self - base()).whole_microseconds()) {
            Ok(time) => time,
            Err(_) => return Err("value too large to transmit".into()),
        };
        types::timestamp_to_sql(time, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for OffsetDateTime {
    fn from_sql(type_: &Type, raw: &[u8]) -> Result<OffsetDateTime, Box<dyn Error + Sync + Send>> {
        let primitive = PrimitiveDateTime::from_sql(type_, raw)?;
        Ok(primitive.assume_utc())
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for OffsetDateTime {
    fn to_sql(
        &self,
        type_: &Type,
        w: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let utc_datetime = self.to_offset(UtcOffset::UTC);
        let date = utc_datetime.date();
        let time = utc_datetime.time();
        let primitive = PrimitiveDateTime::new(date, time);
        primitive.to_sql(type_, w)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Date {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Date, Box<dyn Error + Sync + Send>> {
        let jd = types::date_from_sql(raw)?;
        let julian_day = base().date().julian_day() + i64::from(jd);
        if !date_in_range(julian_day) {
            return Err("value too large to decode".into());
        }
        Ok(base().date() + Duration::days(i64::from(jd)))
    }

    accepts!(DATE);
}

impl ToSql for Date {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let jd = (*self - base().date()).whole_days();
        let jd = i32::try_from(jd).map_err(|_| "value too large to transmit")?;

        types::date_to_sql(jd, w);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Time {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Time, Box<dyn Error + Sync + Send>> {
        let usec = types::time_from_sql(raw)?;
        Ok(time!(00:00:00) + Duration::microseconds(usec))
    }

    accepts!(TIME);
}

impl ToSql for Time {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let delta = *self - time!(00:00:00);
        let time = match i64::try_from(delta.whole_microseconds()) {
            Ok(time) => time,
            Err(_) => return Err("value too large to transmit".into()),
        };
        types::time_to_sql(time, w);
        Ok(IsNull::No)
    }

    accepts!(TIME);
    to_sql_checked!();
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn date_out_of_range_errors() {
        // a value that would land outside `time`'s representable year range must
        // error rather than panic.
        let raw = i32::MAX.to_be_bytes();
        assert!(<Date as FromSql>::from_sql(&Type::DATE, &raw).is_err());
    }

    #[test]
    fn date_in_range_decodes() {
        let raw = 1_000i32.to_be_bytes();
        assert!(<Date as FromSql>::from_sql(&Type::DATE, &raw).is_ok());
    }

    #[test]
    fn timestamp_out_of_range_errors() {
        let raw = 9_000_000_000_000_000_000i64.to_be_bytes();
        assert!(<PrimitiveDateTime as FromSql>::from_sql(&Type::TIMESTAMP, &raw).is_err());
    }

    #[test]
    fn timestamp_in_range_decodes() {
        let raw = 0i64.to_be_bytes();
        assert!(<PrimitiveDateTime as FromSql>::from_sql(&Type::TIMESTAMP, &raw).is_ok());
    }
}
