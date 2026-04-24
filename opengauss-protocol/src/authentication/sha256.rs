//! openGauss SHA256 authentication support.
//!
//! Implements the RFC 5802-inspired SHA256 authentication used by openGauss.

use hmac::{Hmac, KeyInit, Mac};
use pbkdf2::pbkdf2_hmac;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::io;

type HmacSha256 = Hmac<Sha256>;

const HMAC_LENGTH: usize = 32;
const K_LENGTH: usize = 32;

/// Default iteration count for PBKDF2 (from openGauss sha2.h)
pub const DEFAULT_ITERATION_COUNT: u32 = 10000;

/// "Client Key" string (from openGauss sha2.cpp)
const CLIENT_KEY_STRING: &[u8] = b"Client Key";

/// Compute the SHA256 auth response for openGauss.
///
/// Algorithm:
///   1. salt_bytes = hex_decode(random64code) → 32 bytes
///   2. token_bytes = hex_decode(token) → 4 bytes
///   3. k = PBKDF2-HMAC-SHA1(password, salt_bytes, server_iteration) → 32 bytes
///   4. client_key = HMAC-SHA256(k, "Client Key")
///   5. stored_key = SHA256(client_key)
///   6. hmac_result = HMAC-SHA256(stored_key, token_bytes)
///   7. H = hmac_result XOR client_key
///   8. response = hex_encode(H) → 64 hex chars
pub fn rfc5802_algorithm(
    password: &[u8],
    random64code: &[u8; 64],
    token: &[u8; 8],
    server_iteration: i32,
) -> io::Result<String> {
    let salt_bytes = hex_decode(random64code)?;
    let token_bytes = hex_decode(token)?;

    if salt_bytes.len() != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid salt length: expected 32 bytes, got {}",
                salt_bytes.len()
            ),
        ));
    }
    if token_bytes.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "invalid token length: expected 4 bytes, got {}",
                token_bytes.len()
            ),
        ));
    }

    let iterations = if server_iteration > 0 {
        server_iteration as u32
    } else {
        DEFAULT_ITERATION_COUNT
    };

    // Step 1: k = PBKDF2-HMAC-SHA1(password, salt, iterations)
    let mut k = [0u8; K_LENGTH];
    pbkdf2_hmac::<Sha1>(password, &salt_bytes, iterations, &mut k);

    // Step 2: client_key = HMAC-SHA256(k, "Client Key")
    let mut hmac =
        HmacSha256::new_from_slice(&k).expect("HMAC accepts any key size");
    hmac.update(CLIENT_KEY_STRING);
    let client_key = hmac.finalize().into_bytes();

    // Step 3: stored_key = SHA256(client_key)
    let mut hasher = Sha256::new();
    hasher.update(client_key);
    let stored_key = hasher.finalize();

    // Step 4: hmac_result = HMAC-SHA256(stored_key, token_bytes)
    let mut hmac = HmacSha256::new_from_slice(&stored_key)
        .expect("HMAC accepts any key size");
    hmac.update(&token_bytes);
    let hmac_result = hmac.finalize().into_bytes();

    // Step 5: H = hmac_result XOR client_key
    let mut h = [0u8; HMAC_LENGTH];
    for i in 0..HMAC_LENGTH {
        h[i] = hmac_result[i] ^ client_key[i];
    }

    Ok(hex_encode(&h))
}

/// Decode a hex byte slice to bytes
fn hex_decode(hex: &[u8]) -> io::Result<Vec<u8>> {
    let hex_str = std::str::from_utf8(hex)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    hex::decode(hex_str).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
}

/// Encode bytes as lowercase hex string
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_roundtrip() {
        let original = vec![0x0a, 0x0b, 0x0c, 0xff];
        let encoded = hex_encode(&original);
        assert_eq!(encoded, "0a0b0cff");
        let decoded = hex_decode(encoded.as_bytes()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_sha256_auth_output_length() {
        let password = b"testpassword";
        let random64code = [b'0'; 64];
        let token = [b'0'; 8];
        let result =
            rfc5802_algorithm(password, &random64code, &token, 2048).unwrap();
        assert_eq!(
            result.len(),
            64,
            "Output must be 64 hex chars (32 bytes)"
        );
    }

    #[test]
    fn test_sha256_auth_deterministic() {
        let password = b"testpassword";
        let random64code = [b'0'; 64];
        let token = [b'0'; 8];
        let r1 =
            rfc5802_algorithm(password, &random64code, &token, 2048).unwrap();
        let r2 =
            rfc5802_algorithm(password, &random64code, &token, 2048).unwrap();
        assert_eq!(r1, r2, "Same inputs must produce same output");
    }
}
