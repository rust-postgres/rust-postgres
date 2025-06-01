use bytes::BytesMut;
use postgres_protocol::types;
use std::error::Error;
use time_02::{date, time, Date, Duration, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::{FromSql, IsNull, ToSql, Type};

#[rustfmt::skip]
const fn base() -> PrimitiveDateTime {
    PrimitiveDateTime::new(date!(2000-01-01), time!(00:00:00))
}

impl<'a> FromSql<'a> for PrimitiveDateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<PrimitiveDateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
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
