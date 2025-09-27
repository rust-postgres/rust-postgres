use bytes::BytesMut;
use postgres_protocol::types;
use std::error::Error;
use time_03::{Date, Duration, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

use crate::{FromSql, IsNull, ToSql, Type};

fn base() -> PrimitiveDateTime {
    PrimitiveDateTime::new(Date::from_ordinal_date(2000, 1).unwrap(), Time::MIDNIGHT)
}

impl<'a> FromSql<'a> for PrimitiveDateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<PrimitiveDateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base()
            .checked_add(Duration::microseconds(t))
            .ok_or("value too large to decode")?)
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
        Ok(base()
            .date()
            .checked_add(Duration::days(i64::from(jd)))
            .ok_or("value too large to decode")?)
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
        Ok(Time::MIDNIGHT + Duration::microseconds(usec))
    }

    accepts!(TIME);
}

impl ToSql for Time {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let delta = *self - Time::MIDNIGHT;
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
