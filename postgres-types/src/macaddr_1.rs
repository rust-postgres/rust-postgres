use bytes::BytesMut;
use macaddr_1::{MacAddr6, MacAddr8};
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for MacAddr6 {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddr6, Box<dyn Error + Sync + Send>> {
        let bytes = types::macaddr_from_sql(raw)?;
        Ok(MacAddr6::from(bytes))
    }

    accepts!(MACADDR);
}

impl<'a> FromSql<'a> for MacAddr8 {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddr8, Box<dyn Error + Sync + Send>> {
        let bytes = types::macaddr8_from_sql(raw)?;
        Ok(MacAddr8::from(bytes))
    }

    accepts!(MACADDR8);
}

impl ToSql for MacAddr6 {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::macaddr_to_sql(self.into_array(), w);
        Ok(IsNull::No)
    }

    accepts!(MACADDR);
    to_sql_checked!();
}

impl ToSql for MacAddr8 {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::macaddr8_to_sql(self.into_array(), w);
        Ok(IsNull::No)
    }

    accepts!(MACADDR8);
    to_sql_checked!();
}
