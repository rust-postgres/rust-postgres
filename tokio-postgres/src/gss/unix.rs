//! Unix GSSAPI implementation of the Kerberos client context.
//!
//! `libgssapi` binds the *system* GSS library (MIT krb5 or Heimdal). That is
//! deliberate rather than incidental: it is the same library `kinit` populates,
//! so an existing ticket cache and a system `krb5.conf` work with no extra
//! configuration — and no OpenSSL enters the tree.

use super::{GssClientContext, GssError, GssFailure, GssParams, GssStep};
use libgssapi::context::{ClientCtx, CtxFlags, SecurityContext};
use libgssapi::credential::{Cred, CredUsage};
use libgssapi::error::{Error as GssApiError, MajorFlags};
use libgssapi::name::Name;
use libgssapi::oid::{OidSet, GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE, GSS_NT_KRB5_PRINCIPAL};

// MIT krb5's public minor-status values. These are stable ABI (krb5.h's
// `KRB5KDC_ERR_*` / `KRB5_*` error table) and are what the system library
// reports through the GSS minor status.
const KRB5_BASE: i32 = -1_765_328_384;
const KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN: i32 = KRB5_BASE + 6;
const KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN: i32 = KRB5_BASE + 7;
const KRB5KDC_ERR_PREAUTH_FAILED: i32 = KRB5_BASE + 24;
const KRB5KRB_AP_ERR_SKEW: i32 = KRB5_BASE + 37;

/// Classify a GSS failure.
///
/// Two sources are combined on purpose. The GSS **major** flags are portable but
/// coarse (`GSS_S_NO_CRED` is precise, `GSS_S_FAILURE` means "see the minor").
/// The **minor** status is precise but mechanism-specific. Where neither settles
/// it, the library's rendered text is matched — MIT krb5's error table is not
/// localised, so those strings are stable in practice, and a miss only costs the
/// generic message while the verbatim platform text is always included anyway.
fn classify(err: &GssApiError, rendered: &str) -> GssFailure {
    // ORDER MATTERS, and the major flags come LAST.
    //
    // The GSS major for an unreachable KDC is `GSS_S_NO_CRED` — the library
    // genuinely could not obtain a credential — so checking the major first
    // classified it as "no credentials" and told the user to run `kinit`, which
    // is the wrong next step when the KDC cannot be reached at all. Observed
    // 2026-09-01 against a blackholed KDC: major 0x00070000 with the text
    // `Cannot find KDC for realm "VELA.TEST"`. The specific signals must
    // therefore win over the coarse one.
    match err.minor as i32 {
        KRB5KRB_AP_ERR_SKEW => return GssFailure::ClockSkew,
        KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN => return GssFailure::UnknownService,
        KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN | KRB5KDC_ERR_PREAUTH_FAILED => {
            return GssFailure::BadCredentials
        }
        _ => {}
    }

    // Within the text checks the order matters too: the unreachable-KDC message
    // is NESTED inside a "No credentials were supplied…" wrapper, so it contains
    // both vocabularies and the more specific one has to be tested first.
    let lower = rendered.to_ascii_lowercase();
    if lower.contains("clock skew") {
        return GssFailure::ClockSkew;
    }
    if lower.contains("cannot contact any kdc")
        || lower.contains("cannot find kdc")
        || lower.contains("kdc_unreach")
        || lower.contains("unable to reach any kdc")
        || lower.contains("realm not local to kdc")
    {
        return GssFailure::KdcUnreachable;
    }
    if lower.contains("server not found in kerberos database")
        || lower.contains("service principal")
    {
        return GssFailure::UnknownService;
    }
    if lower.contains("password incorrect")
        || lower.contains("preauthentication failed")
        || lower.contains("client not found in kerberos database")
    {
        return GssFailure::BadCredentials;
    }
    if lower.contains("no credentials cache")
        || lower.contains("credentials cache file not found")
        || lower.contains("no credentials")
        || lower.contains("matching credential not found")
    {
        return GssFailure::NoCredentials;
    }

    // Coarse fallback, only once nothing specific matched.
    if err.major.contains(MajorFlags::GSS_S_NO_CRED)
        || err.major.contains(MajorFlags::GSS_S_CREDENTIALS_EXPIRED)
    {
        return GssFailure::NoCredentials;
    }

    GssFailure::Other
}

fn gss_err(err: GssApiError, stage: &'static str) -> GssError {
    // `Display` here calls gss_display_status, i.e. the system library's own
    // human text for both the major and minor codes.
    let rendered = err.to_string();
    let kind = classify(&err, &rendered);
    GssError::new(
        kind,
        stage,
        format!(
            "GSS major 0x{:08X}, minor {}: {}",
            err.major.bits(),
            err.minor,
            rendered.trim()
        ),
    )
}

pub(crate) struct ClientContext {
    ctx: ClientCtx,
}

impl GssClientContext for ClientContext {
    fn new(params: &GssParams<'_>) -> Result<Self, GssError> {
        let mechs = OidSet::singleton(GSS_MECH_KRB5)
            .map_err(|e| gss_err(e, "select the Kerberos 5 mechanism"))?;

        // GSS_NT_HOSTBASED_SERVICE takes `service@host`, NOT the `service/host`
        // spelling used for a krb5 principal or an SSPI target. Canonicalising it
        // against the krb5 mechanism is what turns it into `service/host@REALM`
        // using the local realm mapping.
        let target = Name::new(
            format!("{}@{}", params.service, params.host).as_bytes(),
            Some(GSS_NT_HOSTBASED_SERVICE),
        )
        .map_err(|e| gss_err(e, "build the service principal name"))?;
        let target = target
            .canonicalize(Some(GSS_MECH_KRB5))
            .map_err(|e| gss_err(e, "canonicalise the service principal name"))?;

        let cred = match (params.principal, params.password, params.keytab) {
            // Explicit principal + password. Needed on any host that is not
            // joined to the realm, where the ambient cache is empty.
            (Some(principal), Some(password), _) => {
                let name = Name::new(principal.as_bytes(), Some(GSS_NT_KRB5_PRINCIPAL))
                    .map_err(|e| gss_err(e, "build the client principal name"))?;
                Some(
                    Cred::acquire_with_password(
                        Some(&name),
                        password,
                        None,
                        CredUsage::Initiate,
                        Some(&mechs),
                    )
                    .map_err(|e| gss_err(e, "acquire credentials for the given principal"))?,
                )
            }
            // A client keytab is selected through the environment, which is the
            // only interface the system library exposes for it. Set it before
            // acquiring so the acquisition picks it up.
            (principal, _, Some(keytab)) => {
                // SAFETY: single-threaded with respect to this variable — it is
                // set immediately before the acquire below and read only by the
                // krb5 library on this thread's behalf.
                unsafe {
                    std::env::set_var("KRB5_CLIENT_KTNAME", keytab);
                }
                let name = match principal {
                    Some(p) => Some(
                        Name::new(p.as_bytes(), Some(GSS_NT_KRB5_PRINCIPAL))
                            .map_err(|e| gss_err(e, "build the client principal name"))?,
                    ),
                    None => None,
                };
                Some(
                    Cred::acquire(name.as_ref(), None, CredUsage::Initiate, Some(&mechs))
                        .map_err(|e| gss_err(e, "acquire credentials from the keytab"))?,
                )
            }
            // A principal with no password and no keytab selects that principal
            // out of the existing ticket cache.
            (Some(principal), None, None) => {
                let name = Name::new(principal.as_bytes(), Some(GSS_NT_KRB5_PRINCIPAL))
                    .map_err(|e| gss_err(e, "build the client principal name"))?;
                Some(
                    Cred::acquire(Some(&name), None, CredUsage::Initiate, Some(&mechs))
                        .map_err(|e| gss_err(e, "acquire credentials for the given principal"))?,
                )
            }
            // Ambient: whatever `kinit` left in the default ticket cache.
            (None, _, None) => None,
        };

        Ok(ClientContext {
            ctx: ClientCtx::new(
                cred,
                target,
                CtxFlags::GSS_C_MUTUAL_FLAG,
                Some(GSS_MECH_KRB5),
            ),
        })
    }

    fn step(&mut self, input: Option<&[u8]>) -> Result<GssStep, GssError> {
        // `step` returns Ok(Some(token)) both when another round trip is needed
        // AND on the final step when a last token must still be sent, so the
        // token alone does not tell us whether we are done — `is_complete` does.
        let token = self
            .ctx
            .step(input, None)
            .map_err(|e| gss_err(e, "initialise security context"))?;

        Ok(GssStep {
            token: token.map(|t| t.to_vec()).unwrap_or_default(),
            complete: self.ctx.is_complete(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn krb5_minor_codes_match_the_published_values() {
        // Pinned against krb5.h. A drift here would silently downgrade every
        // recognised failure to the generic message.
        assert_eq!(KRB5KDC_ERR_C_PRINCIPAL_UNKNOWN, -1_765_328_378);
        assert_eq!(KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN, -1_765_328_377);
        assert_eq!(KRB5KDC_ERR_PREAUTH_FAILED, -1_765_328_360);
        assert_eq!(KRB5KRB_AP_ERR_SKEW, -1_765_328_347);
    }

    fn err(major: MajorFlags, minor: u32) -> GssApiError {
        GssApiError { major, minor }
    }

    #[test]
    fn major_no_cred_is_the_fallback_when_nothing_specific_matches() {
        let e = err(MajorFlags::GSS_S_NO_CRED, 0);
        assert_eq!(classify(&e, "anything at all"), GssFailure::NoCredentials);
    }

    /// Regression, observed live 2026-09-01 against a blackholed KDC.
    ///
    /// An unreachable KDC reports major `GSS_S_NO_CRED` — the library really did
    /// fail to get a credential — and the specific reason is only in the nested
    /// text. Checking the major first classified this as "no credentials" and told
    /// the user to run `kinit`, which cannot work when the KDC is unreachable.
    #[test]
    fn unreachable_kdc_is_not_reported_as_missing_credentials() {
        let e = err(MajorFlags::GSS_S_NO_CRED, 0);
        // Verbatim shape of the real message: the KDC reason nested inside the
        // no-credentials wrapper, so both vocabularies are present at once.
        let rendered = "No credentials were supplied, or the credentials were unavailable or \
                        inaccessible (Cannot find KDC for realm \"VELA.TEST\")";
        assert_eq!(classify(&e, rendered), GssFailure::KdcUnreachable);
    }

    /// The companion case, to prove the fix did not simply invert the bug.
    #[test]
    fn genuinely_missing_credentials_still_classify_as_such() {
        let e = err(MajorFlags::GSS_S_NO_CRED, 0);
        let rendered = "No credentials were supplied, or the credentials were unavailable or \
                        inaccessible (No Kerberos credentials available (default cache: \
                        FILE:/tmp/krb5cc_0))";
        assert_eq!(classify(&e, rendered), GssFailure::NoCredentials);
    }

    #[test]
    fn minor_codes_classify_without_text() {
        let e = err(MajorFlags::GSS_S_FAILURE, KRB5KRB_AP_ERR_SKEW as u32);
        assert_eq!(classify(&e, ""), GssFailure::ClockSkew);

        let e = err(
            MajorFlags::GSS_S_FAILURE,
            KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN as u32,
        );
        assert_eq!(classify(&e, ""), GssFailure::UnknownService);
    }

    #[test]
    fn text_fallback_catches_kdc_unreachable() {
        // There is no single portable minor code for this, which is exactly why
        // the text fallback exists.
        let e = err(MajorFlags::GSS_S_FAILURE, 0);
        assert_eq!(
            classify(&e, "Cannot contact any KDC for realm 'VELA.TEST'"),
            GssFailure::KdcUnreachable
        );
        assert_eq!(
            classify(&e, "No credentials cache found"),
            GssFailure::NoCredentials
        );
        assert_eq!(classify(&e, "something unrecognised"), GssFailure::Other);
    }
}
