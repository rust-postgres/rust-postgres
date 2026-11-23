//! SASL-based authentication support.

use base64::Engine;
use base64::display::Base64Display;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, KeyInit, Mac};
use rand::{self, RngExt};
use sha2::{Digest, Sha256};
use std::fmt::{self, Write};
use std::io;
use std::iter;
use std::mem;
use std::str;
use std::sync::Arc;

const NONCE_LENGTH: usize = 24;

/// The maximum SCRAM iteration count the client will accept from the server.
///
/// The iteration count is sent by the server and drives a PBKDF2 loop, so an
/// unbounded value lets a malicious or impersonating server force the client to
/// perform an arbitrary number of HMAC operations before authentication even
/// completes (a denial of service). 100_000 is ~24x the PostgreSQL default of
/// 4096 and matches the default cap the PostgreSQL JDBC driver (pgjdbc) adopted
/// for the same issue (CVE-2026-42198).
const MAX_ITERATION_COUNT: u32 = 100_000;

/// The identifier of the SCRAM-SHA-256 SASL authentication mechanism.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
/// The identifier of the SCRAM-SHA-256-PLUS SASL authentication mechanism.
pub const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

// since postgres passwords are not required to exclude saslprep-prohibited
// characters or even be valid UTF8, we run saslprep if possible and otherwise
// return the raw password.
fn normalize(pass: &[u8]) -> Vec<u8> {
    let pass = match str::from_utf8(pass) {
        Ok(pass) => pass,
        Err(_) => return pass.to_vec(),
    };

    match stringprep::saslprep(pass) {
        Ok(pass) => pass.into_owned().into_bytes(),
        Err(_) => pass.as_bytes().to_vec(),
    }
}

pub(crate) fn hi(str: &[u8], salt: &[u8], i: u32) -> [u8; 32] {
    let mut hmac =
        Hmac::<Sha256>::new_from_slice(str).expect("HMAC is able to accept all key sizes");
    hmac.update(salt);
    hmac.update(&[0, 0, 0, 1]);
    let mut prev = hmac.finalize().into_bytes();

    let mut hi = prev;

    for _ in 1..i {
        let mut hmac = Hmac::<Sha256>::new_from_slice(str).expect("already checked above");
        hmac.update(&prev);
        prev = hmac.finalize().into_bytes();

        for (hi, prev) in hi.iter_mut().zip(prev) {
            *hi ^= prev;
        }
    }

    hi.into()
}

/// The cryptographic operations of a SCRAM-SHA-256 exchange.
///
/// By default `ScramSha256` uses the RustCrypto `sha2` and `hmac` crates and
/// `rand`. An implementation of this trait passed to
/// [`ScramSha256::new_with_crypto`] performs every cryptographic operation of
/// that exchange instead, for example to use a particular or validated
/// cryptographic module. Each method may fail; an error aborts the exchange,
/// and the built-in implementation is never used as a fallback.
///
/// The password passed to `pbkdf2_sha256` is already SASLprep-normalized.
pub trait ScramCryptoProvider: Send + Sync + fmt::Debug {
    /// Fills `out` with cryptographically secure random bytes.
    fn fill_random(&self, out: &mut [u8]) -> io::Result<()>;

    /// Computes SHA-256 of `data`.
    fn sha256(&self, data: &[u8]) -> io::Result<[u8; 32]>;

    /// Computes HMAC-SHA-256 of `data` under `key`.
    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> io::Result<[u8; 32]>;

    /// Derives 32 bytes with PBKDF2-HMAC-SHA-256 (the SCRAM `Hi` function).
    fn pbkdf2_sha256(&self, password: &[u8], salt: &[u8], iterations: u32) -> io::Result<[u8; 32]>;

    /// Compares the expected and received server signatures without a
    /// data-dependent early exit.
    fn constant_time_eq(&self, expected: &[u8], received: &[u8]) -> bool {
        expected.len() == received.len()
            && std::hint::black_box(
                expected
                    .iter()
                    .zip(received)
                    .fold(0u8, |acc, (a, b)| acc | (a ^ b)),
            ) == 0
    }
}

/// The built-in provider: RustCrypto `sha2`/`hmac` and `rand`.
#[derive(Debug)]
struct DefaultCrypto;

impl ScramCryptoProvider for DefaultCrypto {
    fn fill_random(&self, out: &mut [u8]) -> io::Result<()> {
        rand::rng().fill(out);
        Ok(())
    }

    fn sha256(&self, data: &[u8]) -> io::Result<[u8; 32]> {
        Ok(Sha256::digest(data).into())
    }

    fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> io::Result<[u8; 32]> {
        let mut hmac =
            Hmac::<Sha256>::new_from_slice(key).expect("HMAC is able to accept all key sizes");
        hmac.update(data);
        Ok(hmac.finalize().into_bytes().into())
    }

    fn pbkdf2_sha256(&self, password: &[u8], salt: &[u8], iterations: u32) -> io::Result<[u8; 32]> {
        Ok(hi(password, salt, iterations))
    }
}

enum ChannelBindingInner {
    Unrequested,
    Unsupported,
    TlsServerEndPoint(Vec<u8>),
}

/// The channel binding configuration for a SCRAM authentication exchange.
pub struct ChannelBinding(ChannelBindingInner);

impl ChannelBinding {
    /// The server did not request channel binding.
    pub fn unrequested() -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::Unrequested)
    }

    /// The server requested channel binding but the client is unable to provide it.
    pub fn unsupported() -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::Unsupported)
    }

    /// The server requested channel binding and the client will use the `tls-server-end-point`
    /// method.
    pub fn tls_server_end_point(signature: Vec<u8>) -> ChannelBinding {
        ChannelBinding(ChannelBindingInner::TlsServerEndPoint(signature))
    }

    fn gs2_header(&self) -> &'static str {
        match self.0 {
            ChannelBindingInner::Unrequested => "y,,",
            ChannelBindingInner::Unsupported => "n,,",
            ChannelBindingInner::TlsServerEndPoint(_) => "p=tls-server-end-point,,",
        }
    }

    fn cbind_data(&self) -> &[u8] {
        match self.0 {
            ChannelBindingInner::Unrequested | ChannelBindingInner::Unsupported => &[],
            ChannelBindingInner::TlsServerEndPoint(ref buf) => buf,
        }
    }
}

enum State {
    Update {
        nonce: String,
        password: Vec<u8>,
        channel_binding: ChannelBinding,
    },
    Finish {
        salted_password: [u8; 32],
        auth_message: String,
    },
    Done,
}

/// A type which handles the client side of the SCRAM-SHA-256/SCRAM-SHA-256-PLUS authentication
/// process.
///
/// During the authentication process, if the backend sends an `AuthenticationSASL` message which
/// includes `SCRAM-SHA-256` as an authentication mechanism, this type can be used.
///
/// After a `ScramSha256` is constructed, the buffer returned by the `message()` method should be
/// sent to the backend in a `SASLInitialResponse` message along with the mechanism name.
///
/// The server will reply with an `AuthenticationSASLContinue` message. Its contents should be
/// passed to the `update()` method, after which the buffer returned by the `message()` method
/// should be sent to the backend in a `SASLResponse` message.
///
/// The server will reply with an `AuthenticationSASLFinal` message. Its contents should be passed
/// to the `finish()` method, after which the authentication process is complete.
pub struct ScramSha256 {
    crypto: Arc<dyn ScramCryptoProvider>,
    message: String,
    state: State,
}

impl ScramSha256 {
    /// Constructs a new instance which will use the provided password for authentication.
    pub fn new(password: &[u8], channel_binding: ChannelBinding) -> ScramSha256 {
        // rand 0.5's ThreadRng is cryptographically secure
        let mut rng = rand::rng();
        let nonce = (0..NONCE_LENGTH)
            .map(|_| {
                let mut v = rng.random_range(0x21u8..0x7e);
                if v == 0x2c {
                    v = 0x7e
                }
                v as char
            })
            .collect::<String>();

        ScramSha256::new_inner(password, channel_binding, nonce)
    }

    /// Constructs a new instance which will use the provided password for authentication and
    /// `crypto` for every cryptographic operation of the exchange, including the client nonce.
    ///
    /// Errors from `crypto` are returned as they are; there is no fallback to the built-in
    /// implementation.
    pub fn new_with_crypto(
        password: &[u8],
        channel_binding: ChannelBinding,
        crypto: Arc<dyn ScramCryptoProvider>,
    ) -> io::Result<ScramSha256> {
        let mut random = [0u8; NONCE_LENGTH];
        crypto.fill_random(&mut random)?;
        // base64 only uses printable characters other than ',', so it is a valid SCRAM nonce.
        let nonce = STANDARD.encode(random);

        let mut scram = ScramSha256::new_inner(password, channel_binding, nonce);
        scram.crypto = crypto;
        Ok(scram)
    }

    fn new_inner(password: &[u8], channel_binding: ChannelBinding, nonce: String) -> ScramSha256 {
        ScramSha256 {
            crypto: Arc::new(DefaultCrypto),
            message: format!("{}n=,r={}", channel_binding.gs2_header(), nonce),
            state: State::Update {
                nonce,
                password: normalize(password),
                channel_binding,
            },
        }
    }

    /// Returns the message which should be sent to the backend in an `SASLResponse` message.
    pub fn message(&self) -> &[u8] {
        if let State::Done = self.state {
            panic!("invalid SCRAM state");
        }
        self.message.as_bytes()
    }

    /// Updates the state machine with the response from the backend.
    ///
    /// This should be called when an `AuthenticationSASLContinue` message is received.
    pub fn update(&mut self, message: &[u8]) -> io::Result<()> {
        let (client_nonce, password, channel_binding) =
            match mem::replace(&mut self.state, State::Done) {
                State::Update {
                    nonce,
                    password,
                    channel_binding,
                } => (nonce, password, channel_binding),
                _ => return Err(io::Error::other("invalid SCRAM state")),
            };

        let message =
            str::from_utf8(message).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let parsed = Parser::new(message).server_first_message()?;

        if !parsed.nonce.starts_with(&client_nonce) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid nonce"));
        }

        if parsed.iteration_count > MAX_ITERATION_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SCRAM iteration count exceeds the maximum allowed",
            ));
        }

        let salt = match STANDARD.decode(parsed.salt) {
            Ok(salt) => salt,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        let salted_password =
            self.crypto
                .pbkdf2_sha256(&password, &salt, parsed.iteration_count)?;
        let client_key = self.crypto.hmac_sha256(&salted_password, b"Client Key")?;
        let stored_key = self.crypto.sha256(&client_key)?;

        let mut cbind_input = vec![];
        cbind_input.extend(channel_binding.gs2_header().as_bytes());
        cbind_input.extend(channel_binding.cbind_data());
        let cbind_input = STANDARD.encode(&cbind_input);

        self.message.clear();
        write!(&mut self.message, "c={},r={}", cbind_input, parsed.nonce).unwrap();

        let auth_message = format!("n=,r={},{},{}", client_nonce, message, self.message);

        let client_signature = self
            .crypto
            .hmac_sha256(&stored_key, auth_message.as_bytes())?;

        let mut client_proof = client_key;
        for (proof, signature) in client_proof.iter_mut().zip(client_signature) {
            *proof ^= signature;
        }

        write!(
            &mut self.message,
            ",p={}",
            Base64Display::new(&client_proof, &STANDARD)
        )
        .unwrap();

        self.state = State::Finish {
            salted_password,
            auth_message,
        };
        Ok(())
    }

    /// Finalizes the authentication process.
    ///
    /// This should be called when the backend sends an `AuthenticationSASLFinal` message.
    /// Authentication has only succeeded if this method returns `Ok(())`.
    pub fn finish(&mut self, message: &[u8]) -> io::Result<()> {
        let (salted_password, auth_message) = match mem::replace(&mut self.state, State::Done) {
            State::Finish {
                salted_password,
                auth_message,
            } => (salted_password, auth_message),
            _ => return Err(io::Error::other("invalid SCRAM state")),
        };

        let message =
            str::from_utf8(message).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let parsed = Parser::new(message).server_final_message()?;

        let verifier = match parsed {
            ServerFinalMessage::Error(e) => {
                return Err(io::Error::other(format!("SCRAM error: {e}")));
            }
            ServerFinalMessage::Verifier(verifier) => verifier,
        };

        let verifier = match STANDARD.decode(verifier) {
            Ok(verifier) => verifier,
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidInput, e)),
        };

        let server_key = self.crypto.hmac_sha256(&salted_password, b"Server Key")?;
        let expected = self
            .crypto
            .hmac_sha256(&server_key, auth_message.as_bytes())?;
        if self.crypto.constant_time_eq(&expected, &verifier) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SCRAM verification error",
            ))
        }
    }
}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn new(s: &'a str) -> Parser<'a> {
        Parser {
            s,
            it: s.char_indices().peekable(),
        }
    }

    fn eat(&mut self, target: char) -> io::Result<()> {
        match self.it.next() {
            Some((_, c)) if c == target => Ok(()),
            Some((i, c)) => {
                let m =
                    format!("unexpected character at byte {i}: expected `{target}` but got `{c}");
                Err(io::Error::new(io::ErrorKind::InvalidInput, m))
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    fn take_while<F>(&mut self, f: F) -> io::Result<&'a str>
    where
        F: Fn(char) -> bool,
    {
        let start = match self.it.peek() {
            Some(&(i, _)) => i,
            None => return Ok(""),
        };

        loop {
            match self.it.peek() {
                Some(&(_, c)) if f(c) => {
                    self.it.next();
                }
                Some(&(i, _)) => return Ok(&self.s[start..i]),
                None => return Ok(&self.s[start..]),
            }
        }
    }

    fn printable(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| matches!(c, '\x21'..='\x2b' | '\x2d'..='\x7e'))
    }

    fn nonce(&mut self) -> io::Result<&'a str> {
        self.eat('r')?;
        self.eat('=')?;
        self.printable()
    }

    fn base64(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '/' | '+' | '='))
    }

    fn salt(&mut self) -> io::Result<&'a str> {
        self.eat('s')?;
        self.eat('=')?;
        self.base64()
    }

    fn posit_number(&mut self) -> io::Result<u32> {
        let n = self.take_while(|c| c.is_ascii_digit())?;
        n.parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
    }

    fn iteration_count(&mut self) -> io::Result<u32> {
        self.eat('i')?;
        self.eat('=')?;
        self.posit_number()
    }

    fn eof(&mut self) -> io::Result<()> {
        match self.it.peek() {
            Some(&(i, _)) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unexpected trailing data at byte {i}"),
            )),
            None => Ok(()),
        }
    }

    fn server_first_message(&mut self) -> io::Result<ServerFirstMessage<'a>> {
        let nonce = self.nonce()?;
        self.eat(',')?;
        let salt = self.salt()?;
        self.eat(',')?;
        let iteration_count = self.iteration_count()?;
        self.eof()?;

        Ok(ServerFirstMessage {
            nonce,
            salt,
            iteration_count,
        })
    }

    fn value(&mut self) -> io::Result<&'a str> {
        self.take_while(|c| !matches!(c, '\0' | '=' | ','))
    }

    fn server_error(&mut self) -> io::Result<Option<&'a str>> {
        match self.it.peek() {
            Some(&(_, 'e')) => {}
            _ => return Ok(None),
        }

        self.eat('e')?;
        self.eat('=')?;
        self.value().map(Some)
    }

    fn verifier(&mut self) -> io::Result<&'a str> {
        self.eat('v')?;
        self.eat('=')?;
        self.base64()
    }

    fn server_final_message(&mut self) -> io::Result<ServerFinalMessage<'a>> {
        let message = match self.server_error()? {
            Some(error) => ServerFinalMessage::Error(error),
            None => ServerFinalMessage::Verifier(self.verifier()?),
        };
        self.eof()?;
        Ok(message)
    }
}

struct ServerFirstMessage<'a> {
    nonce: &'a str,
    salt: &'a str,
    iteration_count: u32,
}

enum ServerFinalMessage<'a> {
    Error(&'a str),
    Verifier(&'a str),
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_server_first_message() {
        let message = "r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
        let message = Parser::new(message).server_first_message().unwrap();
        assert_eq!(message.nonce, "fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j");
        assert_eq!(message.salt, "QSXCR+Q6sek8bf92");
        assert_eq!(message.iteration_count, 4096);
    }

    #[test]
    fn parse_server_error_message() {
        let message = "e=invalid-proof";
        match Parser::new(message).server_final_message().unwrap() {
            ServerFinalMessage::Error(error) => assert_eq!(error, "invalid-proof"),
            ServerFinalMessage::Verifier(_) => panic!("expected server error"),
        }

        // the error value ends at the first '\0', '=' or ','
        for message in ["invalid-proof\0x", "invalid-proof=x", "invalid-proof,x"] {
            assert_eq!(Parser::new(message).value().unwrap(), "invalid-proof");
        }
    }

    // recorded auth exchange from psql
    #[test]
    fn exchange() {
        let password = "foobar";
        let nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB";

        let client_first = "n,,n=,r=9IZ2O01zb9IgiIZ1WJ/zgpJB";
        let server_first = "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i\
             =4096";
        let client_final = "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS3\
             1NTlQYNs5BTeQjdHdk7lOflDo5re2an8=";
        let server_final = "v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=";

        let mut scram = ScramSha256::new_inner(
            password.as_bytes(),
            ChannelBinding::unsupported(),
            nonce.to_string(),
        );
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_first);

        scram.update(server_first.as_bytes()).unwrap();
        assert_eq!(str::from_utf8(scram.message()).unwrap(), client_final);

        scram.finish(server_final.as_bytes()).unwrap();
    }

    #[test]
    fn excessive_iteration_count_is_rejected() {
        // a malicious server cannot force an unbounded PBKDF2 loop; the iteration
        // count is rejected before `hi()` runs.
        let nonce = "9IZ2O01zb9IgiIZ1WJ/zgpJB";
        let server_first =
            "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i=1000000";

        let mut scram =
            ScramSha256::new_inner(b"foobar", ChannelBinding::unsupported(), nonce.to_string());
        assert!(scram.update(server_first.as_bytes()).is_err());
    }

    /// Delegates to the built-in operations, counts calls, and fails the named operation.
    #[derive(Debug, Default)]
    struct TestCrypto {
        fail: &'static str,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl TestCrypto {
        fn failing(fail: &'static str) -> Arc<TestCrypto> {
            Arc::new(TestCrypto {
                fail,
                ..TestCrypto::default()
            })
        }

        fn call(&self, operation: &str) -> io::Result<()> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if self.fail == operation {
                Err(io::Error::other(operation.to_owned()))
            } else {
                Ok(())
            }
        }
    }

    impl ScramCryptoProvider for TestCrypto {
        fn fill_random(&self, out: &mut [u8]) -> io::Result<()> {
            self.call("random")?;
            out.fill(42);
            Ok(())
        }

        fn sha256(&self, data: &[u8]) -> io::Result<[u8; 32]> {
            self.call("sha256")?;
            DefaultCrypto.sha256(data)
        }

        fn hmac_sha256(&self, key: &[u8], data: &[u8]) -> io::Result<[u8; 32]> {
            self.call("hmac")?;
            DefaultCrypto.hmac_sha256(key, data)
        }

        fn pbkdf2_sha256(
            &self,
            password: &[u8],
            salt: &[u8],
            iterations: u32,
        ) -> io::Result<[u8; 32]> {
            self.call("pbkdf2")?;
            DefaultCrypto.pbkdf2_sha256(password, salt, iterations)
        }
    }

    const NONCE: &str = "9IZ2O01zb9IgiIZ1WJ/zgpJB";
    const SERVER_FIRST: &str =
        "r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,s=fs3IXBy7U7+IvVjZ,i=4096";

    fn exchange_with(crypto: Arc<TestCrypto>) -> ScramSha256 {
        let mut scram =
            ScramSha256::new_inner(b"foobar", ChannelBinding::unsupported(), NONCE.to_string());
        scram.crypto = crypto;
        scram
    }

    #[test]
    fn provider_runs_the_recorded_exchange() {
        let crypto = TestCrypto::failing("");
        let mut scram = exchange_with(crypto.clone());
        scram.update(SERVER_FIRST.as_bytes()).unwrap();
        assert_eq!(
            str::from_utf8(scram.message()).unwrap(),
            "c=biws,r=9IZ2O01zb9IgiIZ1WJ/zgpJBjx/oIRLs02gGSHcw1KEty3eY,p=AmNKosjJzS3\
             1NTlQYNs5BTeQjdHdk7lOflDo5re2an8="
        );
        scram
            .finish(b"v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=")
            .unwrap();
        // pbkdf2, client key hmac, sha256, client signature hmac, server key hmac, server
        // signature hmac
        assert_eq!(crypto.calls.load(std::sync::atomic::Ordering::SeqCst), 6);
    }

    #[test]
    fn provider_rejects_a_wrong_server_signature() {
        for verifier in ["AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "AA=="] {
            let mut scram = exchange_with(TestCrypto::failing(""));
            scram.update(SERVER_FIRST.as_bytes()).unwrap();
            let error = scram
                .finish(format!("v={verifier}").as_bytes())
                .unwrap_err();
            assert_eq!(error.to_string(), "SCRAM verification error");
        }
    }

    #[test]
    fn provider_errors_abort_the_exchange() {
        assert_eq!(
            ScramSha256::new_with_crypto(
                b"foobar",
                ChannelBinding::unsupported(),
                TestCrypto::failing("random"),
            )
            .err()
            .unwrap()
            .to_string(),
            "random"
        );

        for operation in ["pbkdf2", "hmac", "sha256"] {
            let mut scram = exchange_with(TestCrypto::failing(operation));
            let error = scram.update(SERVER_FIRST.as_bytes()).unwrap_err();
            assert_eq!(error.to_string(), operation);
            assert!(scram.update(SERVER_FIRST.as_bytes()).is_err());
        }

        let mut scram = exchange_with(TestCrypto::failing(""));
        scram.update(SERVER_FIRST.as_bytes()).unwrap();
        scram.crypto = TestCrypto::failing("hmac");
        let error = scram
            .finish(b"v=U+ppxD5XUKtradnv8e2MkeupiA8FU87Sg8CXzXHDAzw=")
            .unwrap_err();
        assert_eq!(error.to_string(), "hmac");
    }

    #[test]
    fn provider_nonce_comes_from_fill_random() {
        let scram = ScramSha256::new_with_crypto(
            b"foobar",
            ChannelBinding::unsupported(),
            TestCrypto::failing(""),
        )
        .unwrap();
        assert_eq!(
            str::from_utf8(scram.message()).unwrap(),
            "n,,n=,r=KioqKioqKioqKioqKioqKioqKioqKioq"
        );
    }

    #[test]
    fn default_constant_time_eq() {
        let crypto = DefaultCrypto;
        assert!(crypto.constant_time_eq(b"abc", b"abc"));
        assert!(!crypto.constant_time_eq(b"abc", b"abd"));
        assert!(!crypto.constant_time_eq(b"abc", b"ab"));
        assert!(crypto.constant_time_eq(b"", b""));
    }
}
