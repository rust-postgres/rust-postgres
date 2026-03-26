use std::fmt::{self, Formatter, LowerHex, Write};

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

pub(crate) struct LowerHexWrapper<T>(pub T);

impl<T: AsRef<[u8]>> LowerHex for LowerHexWrapper<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for &b in self.0.as_ref() {
            let [a, b] = encode_byte(b);
            f.write_char(char::from(a))?;
            f.write_char(char::from(b))?;
        }

        Ok(())
    }
}

const fn encode_byte(byte: u8) -> [u8; 2] {
    [lower_nibble_to_hex(byte >> 4), lower_nibble_to_hex(byte)]
}

const fn lower_nibble_to_hex(half_byte: u8) -> u8 {
    HEX_CHARS[(half_byte & 0x0F) as usize]
}
