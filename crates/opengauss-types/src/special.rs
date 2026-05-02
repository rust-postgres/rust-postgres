use bytes::BytesMut;
use opengauss_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

/// A wrapper that can be used to represent infinity with `Type::Date` types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Date<T> {
    /// Represents `infinity`, a date that is later than all other dates.
    PosInfinity,
    /// Represents `-infinity`, a date that is earlier than all other dates.
    NegInfinity,
    /// The wrapped date.
    Value(T),
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Date<T> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match types::date_from_sql(raw)? {
            i32::MAX => Ok(Date::PosInfinity),
            i32::MIN => Ok(Date::NegInfinity),
            _ => T::from_sql(ty, raw).map(Date::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE && T::accepts(ty)
    }
}

impl<T: ToSql> ToSql for Date<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let value = match *self {
            Date::PosInfinity => i32::MAX,
            Date::NegInfinity => i32::MIN,
            Date::Value(ref v) => return v.to_sql(ty, out),
        };

        types::date_to_sql(value, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::DATE && T::accepts(ty)
    }

    to_sql_checked!();
}

/// A wrapper that can be used to represent infinity with `Type::Timestamp` and `Type::Timestamptz`
/// types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp<T> {
    /// Represents `infinity`, a timestamp that is later than all other timestamps.
    PosInfinity,
    /// Represents `-infinity`, a timestamp that is earlier than all other timestamps.
    NegInfinity,
    /// The wrapped timestamp.
    Value(T),
}

impl<'a, T: FromSql<'a>> FromSql<'a> for Timestamp<T> {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match types::timestamp_from_sql(raw)? {
            i64::MAX => Ok(Timestamp::PosInfinity),
            i64::MIN => Ok(Timestamp::NegInfinity),
            _ => T::from_sql(ty, raw).map(Timestamp::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMP | Type::TIMESTAMPTZ if T::accepts(ty))
    }
}

impl<T: ToSql> ToSql for Timestamp<T> {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let value = match *self {
            Timestamp::PosInfinity => i64::MAX,
            Timestamp::NegInfinity => i64::MIN,
            Timestamp::Value(ref v) => return v.to_sql(ty, out),
        };

        types::timestamp_to_sql(value, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::TIMESTAMP | Type::TIMESTAMPTZ if T::accepts(ty))
    }

    to_sql_checked!();
}
