use socket2::TcpKeepalive;
use std::time::Duration;

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct KeepaliveConfig {
    pub idle: Duration,
    pub interval: Option<Duration>,
    pub retries: Option<u32>,
}

impl From<&KeepaliveConfig> for TcpKeepalive {
    fn from(keepalive_config: &KeepaliveConfig) -> Self {
        let mut tcp_keepalive = Self::new().with_time(keepalive_config.idle);

        #[cfg(not(any(
            target_os = "aix",
            target_os = "redox",
            target_os = "solaris",
            target_os = "openbsd"
        )))]
        if let Some(interval) = keepalive_config.interval {
            tcp_keepalive = tcp_keepalive.with_interval(interval);
        }

        #[cfg(not(any(
            target_os = "aix",
            target_os = "redox",
            target_os = "solaris",
            target_os = "windows",
            target_os = "openbsd"
        )))]
        if let Some(retries) = keepalive_config.retries {
            tcp_keepalive = tcp_keepalive.with_retries(retries);
        }

        tcp_keepalive
    }
}
