use std::fmt::Debug;

/// Implementes Buf trait of bytes's crate with Debug
pub trait Buf: bytes::Buf + Debug {}

impl<T> Buf for T where T: bytes::Buf + Debug {}

/// Implementes BufMut trait of bytes's crate with Debug
pub trait BufMut: bytes::BufMut + Debug {}

impl<T> BufMut for T where T: bytes::BufMut + Debug {}