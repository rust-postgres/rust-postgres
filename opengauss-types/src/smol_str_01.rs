use bytes::BytesMut;
use smol_str_01::SmolStr;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for SmolStr {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<SmolStr, Box<dyn Error + Sync + Send>> {
        <&str as FromSql>::from_sql(ty, raw).map(SmolStr::from)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as FromSql>::accepts(ty)
    }
}

impl ToSql for SmolStr {
    fn to_sql(&self, ty: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}
