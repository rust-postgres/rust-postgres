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
    let client_key = hmac_sha256(&k, CLIENT_KEY_STRING);

    // Step 3: stored_key = SHA256(client_key)
    let mut hasher = Sha256::new();
    hasher.update(client_key);
    let stored_key = hasher.finalize();

    // Step 4: hmac_result = HMAC-SHA256(stored_key, token_bytes)
    let hmac_result = hmac_sha256(&stored_key, &token_bytes);

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

/// Helper: HMAC-SHA256(key, data) → 32 bytes
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; HMAC_LENGTH] {
    let mut hmac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key size");
    hmac.update(data);
    hmac.finalize().into_bytes().into()
}

/// Compute the MD5+SHA256 combined auth response for openGauss (AUTH_REQ_MD5_SHA256).
///
/// Algorithm:
///   1. Compute SHA256 encrypted string: pg_sha256_encrypt(password, salt_hex, iteration)
///      This produces: "sha256" + hex(salt) + hex(server_key) + hex(stored_key)
///   2. Take the part after "sha256" prefix (the hex digest)
///   3. MD5 encrypt: "md5" + md5hex(md5(sha256_hex_digest) + md5_salt)
///
/// This is used when the server stores the password as SHA256 but the connection
/// requires MD5 authentication (backward compatibility mode).
pub fn md5_sha256_hash(
    password: &[u8],
    salt_hex: &[u8; 64],
    md5_salt: &[u8; 4],
    iteration_count: i32,
) -> io::Result<String> {
    // Step 1: Compute SHA256 encrypted string
    let salt_bytes = hex_decode(salt_hex)?;
    let iterations = if iteration_count > 0 {
        iteration_count as u32
    } else {
        DEFAULT_ITERATION_COUNT
    };

    let mut k = [0u8; K_LENGTH];
    pbkdf2_hmac::<Sha1>(password, &salt_bytes, iterations, &mut k);

    let server_key = hmac_sha256(&k, b"Sever Key");
    let client_key = hmac_sha256(&k, b"Client Key");

    // stored_key = SHA256(client_key)
    let mut hasher = Sha256::new();
    hasher.update(client_key);
    let stored_key = hasher.finalize();

    // sha256_hex = hex(salt) + hex(server_key) + hex(stored_key)
    let sha256_hex = format!(
        "{}{}{}",
        std::str::from_utf8(salt_hex).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        hex_encode(&server_key),
        hex_encode(&stored_key),
    );

    // Step 2: MD5 encrypt the sha256 hex digest with md5_salt
    // md5(sha256_hex) → inner_hex
    // md5(inner_hex + md5_salt) → result
    use md5::{Digest, Md5};
    use crate::hex::LowerHexWrapper;
    let mut md5 = Md5::new();
    md5.update(sha256_hex.as_bytes());
    let inner = LowerHexWrapper(md5.finalize_reset());

    md5.update(format!("{inner:x}"));
    md5.update(md5_salt);
    Ok(format!("md5{:x}", LowerHexWrapper(md5.finalize())))
}

/// Compute the SM3 auth response for openGauss (AUTH_REQ_SM3).
///
/// SM3 auth is structurally identical to SHA256 auth but uses "Server Key"
/// (correct spelling, 10 chars) instead of "Sever Key" (typo, 9 chars).
/// The PBKDF2 PRF is still SHA1 and the HMAC is still SHA256.
pub fn sm3_algorithm(
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
            format!("invalid salt length: expected 32 bytes, got {}", salt_bytes.len()),
        ));
    }
    if token_bytes.len() != 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid token length: expected 4 bytes, got {}", token_bytes.len()),
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
    let client_key = hmac_sha256(&k, b"Client Key");

    // Step 3: stored_key = SHA256(client_key)
    let mut hasher = Sha256::new();
    hasher.update(client_key);
    let stored_key = hasher.finalize();

    // Step 4: hmac_result = HMAC-SHA256(stored_key, token_bytes)
    let hmac_result = hmac_sha256(&stored_key, &token_bytes);

    // Step 5: H = hmac_result XOR client_key
    let mut h = [0u8; HMAC_LENGTH];
    for i in 0..HMAC_LENGTH {
        h[i] = hmac_result[i] ^ client_key[i];
    }

    Ok(hex_encode(&h))
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

    #[test]
    fn test_sm3_auth_output_length() {
        let password = b"testpassword";
        let random64code = [b'0'; 64];
        let token = [b'0'; 8];
        let result = sm3_algorithm(password, &random64code, &token, 2048).unwrap();
        assert_eq!(result.len(), 64, "Output must be 64 hex chars (32 bytes)");
    }

    #[test]
    fn test_md5_sha256_output_format() {
        let password = b"testpassword";
        let salt_hex = [b'0'; 64];
        let md5_salt = [0u8; 4];
        let result = md5_sha256_hash(password, &salt_hex, &md5_salt, 2048).unwrap();
        assert!(result.starts_with("md5"), "Output should start with 'md5'");
        assert_eq!(result.len(), 35, "md5 + 32 hex chars");
    }
}
