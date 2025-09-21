use crate::{FromSql, Type};
pub use bytes::BytesMut;
use std::error::Error;

pub fn read_be_i32(buf: &mut &[u8]) -> Result<i32, Box<dyn Error + Sync + Send>> {
    let val = buf
        .get(..4)
        .ok_or("invalid buffer size")?
        .try_into()
        .unwrap();
    *buf = &buf[4..];
    Ok(i32::from_be_bytes(val))
}

pub fn read_value<'a, T>(
    type_: &Type,
    buf: &mut &'a [u8],
) -> Result<T, Box<dyn Error + Sync + Send>>
where
    T: FromSql<'a>,
{
    let len = read_be_i32(buf)?;
    let value = if len < 0 {
        None
    } else {
        let (head, tail) = buf
            .split_at_checked(len as usize)
            .ok_or("invalid buffer size")?;
        *buf = tail;
        Some(head)
    };
    T::from_sql_nullable(type_, value)
}
