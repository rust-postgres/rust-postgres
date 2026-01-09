//! Low level Postgres protocol APIs.
//!
//! This crate implements the low level components of Postgres's communication
//! protocol, including message and value serialization and deserialization.
//! It is designed to be used as a building block by higher level APIs such as
//! `rust-postgres`, and should not typically be used directly.
//!
//! # Note
//!
//! This library assumes that the `client_encoding` backend parameter has been
//! set to `UTF8`. It will most likely not behave properly if that is not the case.
#![warn(missing_docs, rust_2018_idioms, clippy::all)]

use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use std::io;

pub mod authentication;
pub mod escape;
pub mod message;
pub mod password;
pub mod types;

/// A Postgres OID.
pub type Oid = u32;

/// A Postgres Log Sequence Number (LSN).
pub type Lsn = u64;

/// An enum indicating if a value is `NULL` or not.
pub enum IsNull {
    /// The value is `NULL`.
    Yes,
    /// The value is not `NULL`.
    No,
}

fn write_nullable<F, E>(serializer: F, buf: &mut BytesMut) -> Result<(), E>
where
    F: FnOnce(&mut BytesMut) -> Result<IsNull, E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.put_i32(0);
    let size = match serializer(buf)? {
        IsNull::No => i32::from_usize(buf.len() - base - 4)?,
        IsNull::Yes => -1,
    };
    BigEndian::write_i32(&mut buf[base..], size);

    Ok(())
}

trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self, io::Error>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::MAX as usize {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large to transmit",
                    ))
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i16);
from_usize!(u16);
from_usize!(i32);

/// Represents a postgres protocol version
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    major: u16,
    minor: u16,
}

impl std::fmt::Debug for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

impl ProtocolVersion {
    /// Version 3.0 of the postgres protocol
    pub const V3_0: ProtocolVersion = ProtocolVersion { major: 3, minor: 0 };
    /// Version 3.2 of the postgres protocol
    pub const V3_2: ProtocolVersion = ProtocolVersion { major: 3, minor: 2 };

    /// Get the major version
    pub fn major(&self) -> u16 {
        self.major
    }

    /// Get the minor version
    pub fn minor(&self) -> u16 {
        self.minor
    }
}
