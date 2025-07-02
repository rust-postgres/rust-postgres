//! Log Sequence Number (LSN) type for PostgreSQL Write-Ahead Log
//! (WAL), also known as the transaction log.

use bytes::BytesMut;
use postgres_protocol::types;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

use crate::{FromSql, IsNull, ToSql, Type};

/// Postgres `PG_LSN` type.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct PgLsn(u64);

/// Error parsing LSN.
#[derive(Debug)]
pub struct ParseLsnError(());

impl From<u64> for PgLsn {
    fn from(lsn_u64: u64) -> Self {
        PgLsn(lsn_u64)
    }
}

impl From<PgLsn> for u64 {
    fn from(lsn: PgLsn) -> u64 {
        lsn.0
    }
}

impl FromStr for PgLsn {
    type Err = ParseLsnError;

    fn from_str(lsn_str: &str) -> Result<Self, Self::Err> {
        let Some((split_hi, split_lo)) = lsn_str.split_once('/') else {
            return Err(ParseLsnError(()));
        };
        let (hi, lo) = (
            u64::from_str_radix(split_hi, 16).map_err(|_| ParseLsnError(()))?,
            u64::from_str_radix(split_lo, 16).map_err(|_| ParseLsnError(()))?,
        );
        Ok(PgLsn((hi << 32) | lo))
    }
}

impl fmt::Display for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0x00000000ffffffff)
    }
}

impl fmt::Debug for PgLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl<'a> FromSql<'a> for PgLsn {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let v = types::lsn_from_sql(raw)?;
        Ok(v.into())
    }

    accepts!(PG_LSN);
}

impl ToSql for PgLsn {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::lsn_to_sql((*self).into(), out);
        Ok(IsNull::No)
    }

    accepts!(PG_LSN);

    to_sql_checked!();
}
