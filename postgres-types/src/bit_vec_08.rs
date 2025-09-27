use bit_vec_08::BitVec;
use bytes::BytesMut;
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for BitVec {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<BitVec, Box<dyn Error + Sync + Send>> {
        let varbit = types::varbit_from_sql(raw)?;
        let mut bitvec = BitVec::from_bytes(varbit.bytes());
        while bitvec.len() > varbit.len() {
            bitvec.pop();
        }

        Ok(bitvec)
    }

    accepts!(BIT, VARBIT);
}

impl ToSql for BitVec {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::varbit_to_sql(self.len(), self.to_bytes().into_iter(), out)?;
        Ok(IsNull::No)
    }

    accepts!(BIT, VARBIT);
    to_sql_checked!();
}
