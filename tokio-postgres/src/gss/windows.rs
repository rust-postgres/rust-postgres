//! Windows SSPI implementation of the Kerberos client context.
//!
//! Uses the OS security provider directly through `windows-sys` — no
//! third-party Kerberos, TLS or crypto crate is involved. The call sequence is
//! the standard one:
//!
//! ```text
//! AcquireCredentialsHandleW("Kerberos", SECPKG_CRED_OUTBOUND, [identity])
//! InitializeSecurityContextW(target = "service/host")   -> token, CONTINUE_NEEDED
//! InitializeSecurityContextW(server token)              -> token, OK
//! ```
//!
//! The package is **"Kerberos", never "Negotiate"**. Negotiate would silently
//! fall back to NTLM, which a Postgres server asking for GSS will reject anyway
//! — but only after the client reported a successful handshake, turning a clear
//! configuration error into a confusing one.

use super::{GssClientContext, GssError, GssFailure, GssParams, GssStep};
use std::ffi::c_void;
use std::ptr;
use windows_sys::Win32::Security::Authentication::Identity::{
    AcquireCredentialsHandleW, DeleteSecurityContext, FreeCredentialsHandle,
    InitializeSecurityContextW, SecBuffer, SecBufferDesc, SECBUFFER_TOKEN, SECBUFFER_VERSION,
    SECPKG_CRED_OUTBOUND,
};
use windows_sys::Win32::Security::Credentials::SecHandle;
// `SEC_WINNT_AUTH_IDENTITY_W` and its UNICODE flag live under System::Rpc in
// windows-sys, not beside the SSPI functions that consume them.
use windows_sys::Win32::System::Rpc::{
    SEC_WINNT_AUTH_IDENTITY_UNICODE, SEC_WINNT_AUTH_IDENTITY_W,
};

// SSPI status codes. Values are from `sspi.h` / `winerror.h` and are stable ABI.
const SEC_E_OK: i32 = 0x0000_0000u32 as i32;
const SEC_I_CONTINUE_NEEDED: i32 = 0x0009_0312u32 as i32;
const SEC_I_COMPLETE_NEEDED: i32 = 0x0009_0313u32 as i32;
const SEC_I_COMPLETE_AND_CONTINUE: i32 = 0x0009_0314u32 as i32;

const SEC_E_TARGET_UNKNOWN: i32 = 0x8009_0303u32 as i32;
const SEC_E_LOGON_DENIED: i32 = 0x8009_030Cu32 as i32;
const SEC_E_NO_CREDENTIALS: i32 = 0x8009_030Eu32 as i32;
const SEC_E_NO_AUTHENTICATING_AUTHORITY: i32 = 0x8009_0311u32 as i32;
const SEC_E_WRONG_PRINCIPAL: i32 = 0x8009_0322u32 as i32;
const SEC_E_TIME_SKEW: i32 = 0x8009_0324u32 as i32;
const SEC_E_UNKNOWN_CREDENTIALS: i32 = 0x8009_031Du32 as i32;

// Context requirements. Mutual auth means the server must prove itself too,
// which is the whole point of using Kerberos over a password.
const ISC_REQ_MUTUAL_AUTH: u32 = 0x0000_0002;
const SECURITY_NATIVE_DREP: u32 = 0x0000_0010;

/// SSPI will not emit a token larger than the package's maximum; 48 KiB is
/// comfortably above the Kerberos maximum even with a large PAC.
const MAX_TOKEN: usize = 48 * 1024;

fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Map an SSPI status onto one of the failure classes users can act on.
fn classify(status: i32) -> GssFailure {
    match status {
        SEC_E_NO_CREDENTIALS | SEC_E_UNKNOWN_CREDENTIALS => GssFailure::NoCredentials,
        SEC_E_TARGET_UNKNOWN | SEC_E_WRONG_PRINCIPAL => GssFailure::UnknownService,
        SEC_E_TIME_SKEW => GssFailure::ClockSkew,
        SEC_E_NO_AUTHENTICATING_AUTHORITY => GssFailure::KdcUnreachable,
        SEC_E_LOGON_DENIED => GssFailure::BadCredentials,
        _ => GssFailure::Other,
    }
}

fn sspi_err(status: i32, stage: &'static str) -> GssError {
    GssError::new(
        classify(status),
        stage,
        format!("SSPI status 0x{:08X}", status as u32),
    )
}

pub(crate) struct ClientContext {
    cred: SecHandle,
    ctx: SecHandle,
    /// `ctx` is only valid once the first `InitializeSecurityContextW` has run.
    ctx_valid: bool,
    target: Vec<u16>,
    /// Kept alive for as long as `cred` is: SSPI does not copy the identity
    /// strings, so dropping these before `FreeCredentialsHandle` would be a
    /// use-after-free.
    _identity_storage: Option<IdentityStorage>,
}

/// Owns the wide strings referenced by `SEC_WINNT_AUTH_IDENTITY_W`.
struct IdentityStorage {
    _user: Vec<u16>,
    _domain: Vec<u16>,
    _password: Vec<u16>,
}

impl Drop for ClientContext {
    fn drop(&mut self) {
        unsafe {
            if self.ctx_valid {
                DeleteSecurityContext(&self.ctx);
            }
            FreeCredentialsHandle(&self.cred);
        }
    }
}

fn empty_handle() -> SecHandle {
    SecHandle {
        dwLower: 0,
        dwUpper: 0,
    }
}

impl GssClientContext for ClientContext {
    fn new(params: &GssParams<'_>) -> Result<Self, GssError> {
        let package = wide("Kerberos");
        let mut cred = empty_handle();
        let mut expiry = 0i64;

        // Split an explicit `user@REALM` into the (user, domain) pair SSPI wants.
        // A principal with no realm is passed through with an empty domain, which
        // lets the default realm apply.
        let (identity, storage) = match params.principal {
            Some(principal) => {
                let (user, domain) = match principal.rsplit_once('@') {
                    Some((u, d)) => (u, d),
                    None => (principal, ""),
                };
                let password = params.password.unwrap_or("");
                let mut user_w = wide(user);
                let mut domain_w = wide(domain);
                let mut password_w = wide(password);

                let id = SEC_WINNT_AUTH_IDENTITY_W {
                    User: user_w.as_mut_ptr(),
                    UserLength: user.encode_utf16().count() as u32,
                    Domain: domain_w.as_mut_ptr(),
                    DomainLength: domain.encode_utf16().count() as u32,
                    Password: password_w.as_mut_ptr(),
                    PasswordLength: password.encode_utf16().count() as u32,
                    Flags: SEC_WINNT_AUTH_IDENTITY_UNICODE,
                };
                (
                    Some(id),
                    Some(IdentityStorage {
                        _user: user_w,
                        _domain: domain_w,
                        _password: password_w,
                    }),
                )
            }
            // Ambient: the logon session's own Kerberos credentials.
            None => (None, None),
        };

        let status = unsafe {
            AcquireCredentialsHandleW(
                ptr::null(),
                package.as_ptr(),
                SECPKG_CRED_OUTBOUND,
                ptr::null(),
                identity
                    .as_ref()
                    .map_or(ptr::null(), |id| id as *const _ as *const c_void),
                None,
                ptr::null(),
                &mut cred,
                &mut expiry,
            )
        };
        if status != SEC_E_OK {
            return Err(sspi_err(status, "acquire credentials"));
        }

        Ok(ClientContext {
            cred,
            ctx: empty_handle(),
            ctx_valid: false,
            // SSPI's target is the `service/host` spelling; the realm is resolved
            // from the host via the realm mapping, so it is not spelled here.
            target: wide(&params.spn()),
            _identity_storage: storage,
        })
    }

    fn step(&mut self, input: Option<&[u8]>) -> Result<GssStep, GssError> {
        let mut out_token = vec![0u8; MAX_TOKEN];
        let mut out_buf = SecBuffer {
            cbBuffer: MAX_TOKEN as u32,
            BufferType: SECBUFFER_TOKEN,
            pvBuffer: out_token.as_mut_ptr() as *mut c_void,
        };
        let mut out_desc = SecBufferDesc {
            ulVersion: SECBUFFER_VERSION,
            cBuffers: 1,
            pBuffers: &mut out_buf,
        };

        // The input descriptor must be absent on the first call, not merely empty.
        let mut in_buf;
        let mut in_desc;
        let in_ptr = match input {
            Some(tok) => {
                in_buf = SecBuffer {
                    cbBuffer: tok.len() as u32,
                    BufferType: SECBUFFER_TOKEN,
                    pvBuffer: tok.as_ptr() as *mut c_void,
                };
                in_desc = SecBufferDesc {
                    ulVersion: SECBUFFER_VERSION,
                    cBuffers: 1,
                    pBuffers: &mut in_buf,
                };
                &mut in_desc as *mut SecBufferDesc as *const SecBufferDesc
            }
            None => ptr::null(),
        };

        let mut attrs = 0u32;
        let mut expiry = 0i64;
        let existing = if self.ctx_valid {
            &self.ctx as *const SecHandle
        } else {
            ptr::null()
        };

        let status = unsafe {
            InitializeSecurityContextW(
                &self.cred,
                existing,
                self.target.as_ptr(),
                ISC_REQ_MUTUAL_AUTH,
                0,
                SECURITY_NATIVE_DREP,
                in_ptr,
                0,
                &mut self.ctx,
                &mut out_desc,
                &mut attrs,
                &mut expiry,
            )
        };

        match status {
            SEC_E_OK | SEC_I_CONTINUE_NEEDED | SEC_I_COMPLETE_NEEDED
            | SEC_I_COMPLETE_AND_CONTINUE => {
                self.ctx_valid = true;
                out_token.truncate(out_buf.cbBuffer as usize);
                Ok(GssStep {
                    token: out_token,
                    // COMPLETE_NEEDED/AND_CONTINUE relate to CompleteAuthToken,
                    // which the Kerberos package does not require; only
                    // CONTINUE_NEEDED means another server token is coming.
                    complete: status != SEC_I_CONTINUE_NEEDED
                        && status != SEC_I_COMPLETE_AND_CONTINUE,
                })
            }
            other => Err(sspi_err(other, "initialise security context")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_covers_the_four_actionable_modes() {
        assert_eq!(classify(SEC_E_NO_CREDENTIALS), GssFailure::NoCredentials);
        assert_eq!(classify(SEC_E_TARGET_UNKNOWN), GssFailure::UnknownService);
        assert_eq!(classify(SEC_E_TIME_SKEW), GssFailure::ClockSkew);
        assert_eq!(
            classify(SEC_E_NO_AUTHENTICATING_AUTHORITY),
            GssFailure::KdcUnreachable
        );
        assert_eq!(classify(SEC_E_LOGON_DENIED), GssFailure::BadCredentials);
        assert_eq!(classify(0x7FFF_0000), GssFailure::Other);
    }

    #[test]
    fn status_codes_are_negative_hresults() {
        // A sign slip here would silently turn every failure into `Other`, so
        // pin the representation rather than the arithmetic.
        assert!(SEC_E_TIME_SKEW < 0);
        assert!(SEC_I_CONTINUE_NEEDED > 0);
        assert_eq!(SEC_E_OK, 0);
    }

    #[test]
    fn wide_is_nul_terminated() {
        let w = wide("ab");
        assert_eq!(w, vec![b'a' as u16, b'b' as u16, 0]);
    }

    #[test]
    fn error_message_carries_the_status() {
        let e = sspi_err(SEC_E_TIME_SKEW, "initialise security context");
        assert_eq!(e.kind(), GssFailure::ClockSkew);
        assert!(e.to_string().contains("0x80090324"), "{e}");
    }
}
