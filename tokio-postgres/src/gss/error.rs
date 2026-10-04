//! Turning platform Kerberos status codes into messages a user can act on.
//!
//! A raw GSS minor status or SSPI `HRESULT` is not an error message. Every
//! failure here names the resolved SPN and, where the cause is recognised, the
//! concrete next step — because the four things that actually go wrong (no
//! ticket, wrong SPN, clock skew, unreachable KDC) are indistinguishable to a
//! user from the numeric code alone.

use super::GssParams;
use std::error;
use std::fmt;

/// The recognised failure classes, in the order a user is likely to hit them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GssFailure {
    /// No usable credentials: empty ticket cache, or no Windows logon for the realm.
    NoCredentials,
    /// The KDC has no such service principal — nearly always a short-vs-FQDN SPN.
    UnknownService,
    /// Client and KDC clocks disagree by more than the realm's tolerance.
    ClockSkew,
    /// The KDC could not be reached at all.
    KdcUnreachable,
    /// Explicit credentials were rejected (bad password, or unknown principal).
    BadCredentials,
    /// A hostname is required to build an SPN and none was available.
    NoHostname,
    /// Recognised as a failure, but not as one of the above.
    Other,
}

impl GssFailure {
    /// The actionable half of the message. Kept separate from the raw platform
    /// text so the two never blur together.
    fn advice(self) -> &'static str {
        match self {
            GssFailure::NoCredentials => {
                "no Kerberos credentials are available. On Linux/macOS run `kinit <principal>` \
                 first, or supply a principal and password (or a keytab) on the connection. On \
                 Windows, either log on to the domain or supply a principal and password — a \
                 machine that is not joined to the realm has an empty ticket cache and cannot \
                 use ambient credentials"
            }
            GssFailure::UnknownService => {
                "the KDC has no such service principal. This is almost always the short-vs-FQDN \
                 spelling of the host: the SPN must match what was registered on the server \
                 (commonly `postgres/<fully-qualified-host>`). Check the host exactly as typed, \
                 and the `krbsrvname` service if the server does not use the default `postgres`"
            }
            GssFailure::ClockSkew => {
                "the clocks of this machine and the KDC differ by more than the realm allows \
                 (usually 5 minutes). Synchronise the system clock and retry"
            }
            GssFailure::KdcUnreachable => {
                "the KDC for the realm could not be reached. Check that the realm is configured \
                 (krb5.conf / krb5.ini, or `ksetup /addkdc` on Windows), that its KDC address is \
                 correct, and that UDP/TCP port 88 is reachable"
            }
            GssFailure::BadCredentials => {
                "the principal or password was rejected by the KDC. Check the principal spelling \
                 including its realm, which is case-sensitive and conventionally uppercase"
            }
            GssFailure::NoHostname => {
                "Kerberos authentication needs a hostname to build the service principal name, \
                 and this connection has none. Connect by host rather than over a pre-established \
                 stream, or set `krbsrvname` together with an explicit host"
            }
            GssFailure::Other => {
                "Kerberos authentication failed. The platform detail below is the authoritative \
                 cause"
            }
        }
    }
}

/// A Kerberos failure, with the SPN attached once it is known.
#[derive(Clone, Debug)]
pub(crate) struct GssError {
    kind: GssFailure,
    /// Which call failed, e.g. "acquire credentials".
    stage: &'static str,
    /// The platform's own text and/or numeric code, verbatim.
    detail: String,
    /// Resolved SPN, attached by `with_spn` on the way out.
    spn: Option<String>,
}

impl GssError {
    pub(crate) fn new(kind: GssFailure, stage: &'static str, detail: impl Into<String>) -> Self {
        GssError {
            kind,
            stage,
            detail: detail.into(),
            spn: None,
        }
    }

    pub(crate) fn no_hostname() -> Self {
        GssError::new(
            GssFailure::NoHostname,
            "resolve service principal name",
            "no hostname on the connection",
        )
    }

    /// Attach the resolved SPN. Every error leaving this module goes through
    /// here, which is what makes "the message includes the resolved SPN" true by
    /// construction rather than by remembering to do it at each call site.
    pub(crate) fn with_spn(mut self, params: &GssParams<'_>) -> Self {
        if self.kind != GssFailure::NoHostname {
            self.spn = Some(params.spn());
        }
        self
    }

    pub(crate) fn kind(&self) -> GssFailure {
        self.kind
    }
}

impl fmt::Display for GssError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Kerberos authentication failed")?;
        if let Some(spn) = &self.spn {
            write!(f, " for service principal `{spn}`")?;
        }
        write!(f, ": could not {}. ", self.stage)?;
        write!(f, "{}", self.kind.advice())?;
        if !self.detail.is_empty() {
            write!(f, " [{}]", self.detail)?;
        }
        Ok(())
    }
}

impl error::Error for GssError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn params<'a>(service: &'a str, host: &'a str) -> GssParams<'a> {
        GssParams {
            service,
            host,
            principal: None,
            password: None,
            keytab: None,
        }
    }

    #[test]
    fn message_names_the_resolved_spn() {
        let e = GssError::new(GssFailure::UnknownService, "initialise context", "code 0x1")
            .with_spn(&params("postgres", "pg-krb.vela.test"));
        let msg = e.to_string();
        // The SPN is the one thing a user needs to compare against the server,
        // so its absence is a defect, not a cosmetic issue.
        assert!(
            msg.contains("postgres/pg-krb.vela.test"),
            "SPN missing from: {msg}"
        );
        assert!(msg.contains("short-vs-FQDN"), "advice missing from: {msg}");
        assert!(msg.contains("code 0x1"), "platform detail missing: {msg}");
    }

    #[test]
    fn krbsrvname_override_shows_in_the_spn() {
        let e = GssError::new(GssFailure::UnknownService, "initialise context", "")
            .with_spn(&params("pgsql", "db.example.com"));
        assert!(e.to_string().contains("pgsql/db.example.com"));
        // `kind` survives `with_spn` — the platform modules classify once, and
        // attaching the SPN must not reinterpret it.
        assert_eq!(e.kind(), GssFailure::UnknownService);
    }

    #[test]
    fn the_four_failure_modes_have_distinct_messages() {
        let p = params("postgres", "h");
        let msgs: Vec<String> = [
            GssFailure::NoCredentials,
            GssFailure::UnknownService,
            GssFailure::ClockSkew,
            GssFailure::KdcUnreachable,
        ]
        .iter()
        .map(|k| {
            GssError::new(*k, "initialise context", "")
                .with_spn(&p)
                .to_string()
        })
        .collect();

        for (i, a) in msgs.iter().enumerate() {
            for (j, b) in msgs.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "failure modes {i} and {j} produce the same message");
                }
            }
        }
        // And each says something specific, not just "failed".
        assert!(msgs[0].contains("kinit"));
        assert!(msgs[1].contains("service principal"));
        assert!(msgs[2].contains("clock"));
        assert!(msgs[3].contains("port 88"));
    }

    #[test]
    fn no_hostname_does_not_claim_an_spn() {
        // `postgres/` with an empty host would be worse than saying there is none.
        let e = GssError::no_hostname().with_spn(&params("postgres", ""));
        let msg = e.to_string();
        assert!(!msg.contains("postgres/"), "invented an SPN: {msg}");
        assert!(msg.contains("needs a hostname"));
    }
}
