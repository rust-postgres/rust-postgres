//! Connection configuration.

#![allow(clippy::doc_overindented_list_items)]

#[cfg(feature = "runtime")]
use crate::connect::connect;
use crate::connect_raw::connect_raw;
#[cfg(not(target_arch = "wasm32"))]
use crate::keepalive::KeepaliveConfig;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{Client, Connection, Error};
use std::borrow::Cow;
#[cfg(unix)]
use std::ffi::OsStr;
use std::net::IpAddr;
use std::ops::Deref;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::path::{Path, PathBuf};
use std::str;
use std::str::FromStr;
use std::time::Duration;
use std::{error, fmt, iter, mem};
use tokio::io::{AsyncRead, AsyncWrite};

/// Properties required of a session.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TargetSessionAttrs {
    /// No special properties are required.
    Any,
    /// The session must allow writes.
    ReadWrite,
    /// The session allow only reads.
    ReadOnly,
}

/// TLS configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SslMode {
    /// Do not use TLS.
    Disable,
    /// Attempt to connect with TLS but allow sessions without.
    Prefer,
    /// Require the use of TLS.
    Require,
}

/// TLS negotiation configuration
///
/// See more information at
/// https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-SSLNEGOTIATION
#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum SslNegotiation {
    /// Use PostgreSQL SslRequest for Ssl negotiation
    #[default]
    Postgres,
    /// Start Ssl handshake without negotiation, only works for PostgreSQL 17+
    Direct,
}

/// Channel binding configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ChannelBinding {
    /// Do not use channel binding.
    Disable,
    /// Attempt to use channel binding but allow sessions without.
    Prefer,
    /// Require the use of channel binding.
    Require,
}

/// Load balancing configuration.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoadBalanceHosts {
    /// Make connection attempts to hosts in the order provided.
    Disable,
    /// Make connection attempts to hosts in a random order.
    Random,
}

/// A host specification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Host {
    /// A TCP hostname.
    Tcp(String),
    /// A path to a directory containing the server's Unix socket.
    ///
    /// This variant is only available on Unix platforms.
    #[cfg(unix)]
    Unix(PathBuf),
}

/// Connection configuration.
///
/// Configuration can be parsed from libpq-style connection strings. These strings come in two formats:
///
/// # Key-Value
///
/// This format consists of space-separated key-value pairs. Values which are either the empty string or contain
/// whitespace should be wrapped in `'`. `'` and `\` characters should be backslash-escaped.
///
/// ## Keys
///
/// * `user` - The username to authenticate with. Defaults to the user executing this process.
/// * `password` - The password to authenticate with.
/// * `dbname` - The name of the database to connect to. Defaults to the username.
/// * `options` - Command line options used to configure the server.
/// * `application_name` - Sets the `application_name` parameter on the server.
/// * `sslmode` - Controls usage of TLS. If set to `disable`, TLS will not be used. If set to `prefer`, TLS will be used
///     if available, but not used otherwise. If set to `require`, TLS will be forced to be used. Defaults to `prefer`.
/// * `host` - The host to connect to. On Unix platforms, if the host starts with a `/` character it is treated as the
///     path to the directory containing Unix domain sockets. Otherwise, it is treated as a hostname. Multiple hosts
///     can be specified, separated by commas. Each host will be tried in turn when connecting. Required if connecting
///     with the `connect` method.
/// * `sslnegotiation` - TLS negotiation method. If set to `direct`, the client
///     will perform direct TLS handshake, this only works for PostgreSQL 17 and
///     newer.
///     Note that you will need to setup ALPN of TLS client configuration to
///     `postgresql` when using direct TLS. If you are using postgres_openssl
///     as TLS backend, a `postgres_openssl::set_postgresql_alpn` helper is
///     provided for that.
///     If set to `postgres`, the default value, it follows original postgres
///     wire protocol to perform the negotiation.
/// * `hostaddr` - Numeric IP address of host to connect to. This should be in the standard IPv4 address format,
///     e.g., 172.28.40.9. If your machine supports IPv6, you can also use those addresses.
///     If this parameter is not specified, the value of `host` will be looked up to find the corresponding IP address,
///     or if host specifies an IP address, that value will be used directly.
///     Using `hostaddr` allows the application to avoid a host name look-up, which might be important in applications
///     with time constraints. However, a host name is required for TLS certificate verification.
///     Specifically:
///         * If `hostaddr` is specified without `host`, the value for `hostaddr` gives the server network address.
///             The connection attempt will fail if the authentication method requires a host name;
///         * If `host` is specified without `hostaddr`, a host name lookup occurs;
///         * If both `host` and `hostaddr` are specified, the value for `hostaddr` gives the server network address.
///             The value for `host` is ignored unless the authentication method requires it,
///             in which case it will be used as the host name.
/// * `port` - The port to connect to. Multiple ports can be specified, separated by commas. The number of ports must be
///     either 1, in which case it will be used for all hosts, or the same as the number of hosts. Defaults to 5432 if
///     omitted or the empty string.
/// * `connect_timeout` - The time limit in seconds applied to each socket-level connection attempt. Note that hostnames
///     can resolve to multiple IP addresses, and this limit is applied to each address. Defaults to no timeout.
/// * `tcp_user_timeout` - The time limit that transmitted data may remain unacknowledged before a connection is forcibly closed.
///     This is ignored for Unix domain socket connections. It is only supported on systems where TCP_USER_TIMEOUT is available
///     and will default to the system default if omitted or set to 0; on other systems, it has no effect.
/// * `keepalives` - Controls the use of TCP keepalive. A value of 0 disables keepalive and nonzero integers enable it.
///     This option is ignored when connecting with Unix sockets. Defaults to on.
/// * `keepalives_idle` - The number of seconds of inactivity after which a keepalive message is sent to the server.
///     This option is ignored when connecting with Unix sockets. Defaults to 2 hours.
/// * `keepalives_interval` - The time interval between TCP keepalive probes.
///     This option is ignored when connecting with Unix sockets.
/// * `keepalives_retries` - The maximum number of TCP keepalive probes that will be sent before dropping a connection.
///     This option is ignored when connecting with Unix sockets.
/// * `target_session_attrs` - Specifies requirements of the session. If set to `read-write`, the client will check that
///     the `transaction_read_write` session parameter is set to `on`. This can be used to connect to the primary server
///     in a database cluster as opposed to the secondary read-only mirrors. Defaults to `all`.
/// * `channel_binding` - Controls usage of channel binding in the authentication process. If set to `disable`, channel
///     binding will not be used. If set to `prefer`, channel binding will be used if available, but not used otherwise.
///     If set to `require`, the authentication process will fail if channel binding is not used. Defaults to `prefer`.
/// * `load_balance_hosts` - Controls the order in which the client tries to connect to the available hosts and
///     addresses. Once a connection attempt is successful no other hosts and addresses will be tried. This parameter
///     is typically used in combination with multiple host names or a DNS record that returns multiple IPs. If set to
///     `disable`, hosts and addresses will be tried in the order provided. If set to `random`, hosts will be tried
///     in a random order, and the IP addresses resolved from a hostname will also be tried in a random order. Defaults
///     to `disable`.
///
/// ## Examples
///
/// ```not_rust
/// host=localhost user=postgres connect_timeout=10 keepalives=0
/// ```
///
/// ```not_rust
/// host=/var/run/postgresql,localhost port=1234 user=postgres password='password with spaces'
/// ```
///
/// ```not_rust
/// host=host1,host2,host3 port=1234,,5678 hostaddr=127.0.0.1,127.0.0.2,127.0.0.3 user=postgres target_session_attrs=read-write
/// ```
///
/// ```not_rust
/// host=host1,host2,host3 port=1234,,5678 user=postgres target_session_attrs=read-write
/// ```
///
/// # Url
///
/// This format resembles a URL with a scheme of either `postgres://` or `postgresql://`. All components are optional,
/// and the format accepts query parameters for all of the key-value pairs described in the section above. Multiple
/// host/port pairs can be comma-separated. Unix socket paths in the host section of the URL should be percent-encoded,
/// as the path component of the URL specifies the database name.
///
/// ## Examples
///
/// ```not_rust
/// postgresql://user@localhost
/// ```
///
/// ```not_rust
/// postgresql://user:password@%2Fvar%2Frun%2Fpostgresql/mydb?connect_timeout=10
/// ```
///
/// ```not_rust
/// postgresql://user@host1:1234,host2,host3:5678?target_session_attrs=read-write
/// ```
///
/// ```not_rust
/// postgresql:///mydb?user=user&host=/var/run/postgresql
/// ```
#[derive(Clone, PartialEq, Eq)]
pub struct Config {
    pub(crate) user: Option<String>,
    pub(crate) password: Option<Vec<u8>>,
    pub(crate) dbname: Option<String>,
    pub(crate) options: Option<String>,
    pub(crate) application_name: Option<String>,
    pub(crate) ssl_mode: SslMode,
    pub(crate) ssl_negotiation: SslNegotiation,
    pub(crate) host: Vec<Host>,
    pub(crate) hostaddr: Vec<IpAddr>,
    pub(crate) port: Vec<u16>,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) tcp_user_timeout: Option<Duration>,
    pub(crate) keepalives: bool,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) keepalive_config: KeepaliveConfig,
    pub(crate) target_session_attrs: TargetSessionAttrs,
    pub(crate) channel_binding: ChannelBinding,
    pub(crate) load_balance_hosts: LoadBalanceHosts,
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

impl Config {
    /// Creates a new configuration.
    pub fn new() -> Config {
        Config {
            user: None,
            password: None,
            dbname: None,
            options: None,
            application_name: None,
            ssl_mode: SslMode::Prefer,
            ssl_negotiation: SslNegotiation::Postgres,
            host: vec![],
            hostaddr: vec![],
            port: vec![],
            connect_timeout: None,
            tcp_user_timeout: None,
            keepalives: true,
            #[cfg(not(target_arch = "wasm32"))]
            keepalive_config: KeepaliveConfig {
                idle: Duration::from_secs(2 * 60 * 60),
                interval: None,
                retries: None,
            },
            target_session_attrs: TargetSessionAttrs::Any,
            channel_binding: ChannelBinding::Prefer,
            load_balance_hosts: LoadBalanceHosts::Disable,
        }
    }

    /// Sets the user to authenticate with.
    ///
    /// Defaults to the user executing this process.
    pub fn user(&mut self, user: impl Into<String>) -> &mut Config {
        self.user = Some(user.into());
        self
    }

    /// Gets the user to authenticate with, if one has been configured with
    /// the `user` method.
    pub fn get_user(&self) -> Option<&str> {
        self.user.as_deref()
    }

    /// Sets the password to authenticate with.
    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.password = Some(password.as_ref().to_vec());
        self
    }

    /// Gets the password to authenticate with, if one has been configured with
    /// the `password` method.
    pub fn get_password(&self) -> Option<&[u8]> {
        self.password.as_deref()
    }

    /// Sets the name of the database to connect to.
    ///
    /// Defaults to the user.
    pub fn dbname(&mut self, dbname: impl Into<String>) -> &mut Config {
        self.dbname = Some(dbname.into());
        self
    }

    /// Gets the name of the database to connect to, if one has been configured
    /// with the `dbname` method.
    pub fn get_dbname(&self) -> Option<&str> {
        self.dbname.as_deref()
    }

    /// Sets command line options used to configure the server.
    pub fn options(&mut self, options: impl Into<String>) -> &mut Config {
        self.options = Some(options.into());
        self
    }

    /// Gets the command line options used to configure the server, if the
    /// options have been set with the `options` method.
    pub fn get_options(&self) -> Option<&str> {
        self.options.as_deref()
    }

    /// Sets the value of the `application_name` runtime parameter.
    pub fn application_name(&mut self, application_name: impl Into<String>) -> &mut Config {
        self.application_name = Some(application_name.into());
        self
    }

    /// Gets the value of the `application_name` runtime parameter, if it has
    /// been set with the `application_name` method.
    pub fn get_application_name(&self) -> Option<&str> {
        self.application_name.as_deref()
    }

    /// Sets the SSL configuration.
    ///
    /// Defaults to `prefer`.
    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.ssl_mode = ssl_mode;
        self
    }

    /// Gets the SSL configuration.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.ssl_mode
    }

    /// Sets the SSL negotiation method.
    ///
    /// Defaults to `postgres`.
    pub fn ssl_negotiation(&mut self, ssl_negotiation: SslNegotiation) -> &mut Config {
        self.ssl_negotiation = ssl_negotiation;
        self
    }

    /// Gets the SSL negotiation method.
    pub fn get_ssl_negotiation(&self) -> SslNegotiation {
        self.ssl_negotiation
    }

    /// Adds a host to the configuration.
    ///
    /// Multiple hosts can be specified by calling this method multiple times, and each will be tried in order. On Unix
    /// systems, a host starting with a `/` is interpreted as a path to a directory containing Unix domain sockets.
    /// There must be either no hosts, or the same number of hosts as hostaddrs.
    pub fn host(&mut self, host: impl Into<String>) -> &mut Config {
        let host = host.into();

        #[cfg(unix)]
        {
            if host.starts_with('/') {
                return self.host_path(host);
            }
        }

        self.host.push(Host::Tcp(host));
        self
    }

    /// Gets the hosts that have been added to the configuration with `host`.
    pub fn get_hosts(&self) -> &[Host] {
        &self.host
    }

    /// Gets the hostaddrs that have been added to the configuration with `hostaddr`.
    pub fn get_hostaddrs(&self) -> &[IpAddr] {
        self.hostaddr.deref()
    }

    /// Adds a Unix socket host to the configuration.
    ///
    /// Unlike `host`, this method allows non-UTF8 paths.
    #[cfg(unix)]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        self.host.push(Host::Unix(host.as_ref().to_path_buf()));
        self
    }

    /// Adds a hostaddr to the configuration.
    ///
    /// Multiple hostaddrs can be specified by calling this method multiple times, and each will be tried in order.
    /// There must be either no hostaddrs, or the same number of hostaddrs as hosts.
    pub fn hostaddr(&mut self, hostaddr: IpAddr) -> &mut Config {
        self.hostaddr.push(hostaddr);
        self
    }

    /// Adds a port to the configuration.
    ///
    /// Multiple ports can be specified by calling this method multiple times. There must either be no ports, in which
    /// case the default of 5432 is used, a single port, in which it is used for all hosts, or the same number of ports
    /// as hosts.
    pub fn port(&mut self, port: u16) -> &mut Config {
        self.port.push(port);
        self
    }

    /// Gets the ports that have been added to the configuration with `port`.
    pub fn get_ports(&self) -> &[u16] {
        &self.port
    }

    /// Sets the timeout applied to socket-level connection attempts.
    ///
    /// Note that hostnames can resolve to multiple IP addresses, and this timeout will apply to each address of each
    /// host separately. Defaults to no limit.
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.connect_timeout = Some(connect_timeout);
        self
    }

    /// Gets the connection timeout, if one has been set with the
    /// `connect_timeout` method.
    pub fn get_connect_timeout(&self) -> Option<&Duration> {
        self.connect_timeout.as_ref()
    }

    /// Sets the TCP user timeout.
    ///
    /// This is ignored for Unix domain socket connections. It is only supported on systems where
    /// TCP_USER_TIMEOUT is available and will default to the system default if omitted or set to 0;
    /// on other systems, it has no effect.
    pub fn tcp_user_timeout(&mut self, tcp_user_timeout: Duration) -> &mut Config {
        self.tcp_user_timeout = Some(tcp_user_timeout);
        self
    }

    /// Gets the TCP user timeout, if one has been set with the
    /// `user_timeout` method.
    pub fn get_tcp_user_timeout(&self) -> Option<&Duration> {
        self.tcp_user_timeout.as_ref()
    }

    /// Controls the use of TCP keepalive.
    ///
    /// This is ignored for Unix domain socket connections. Defaults to `true`.
    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        self.keepalives = keepalives;
        self
    }

    /// Reports whether TCP keepalives will be used.
    pub fn get_keepalives(&self) -> bool {
        self.keepalives
    }

    /// Sets the amount of idle time before a keepalive packet is sent on the connection.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled. Defaults to 2 hours.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        self.keepalive_config.idle = keepalives_idle;
        self
    }

    /// Gets the configured amount of idle time before a keepalive packet will
    /// be sent on the connection.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_keepalives_idle(&self) -> Duration {
        self.keepalive_config.idle
    }

    /// Sets the time interval between TCP keepalive probes.
    /// On Windows, this sets the value of the tcp_keepalive struct’s keepaliveinterval field.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn keepalives_interval(&mut self, keepalives_interval: Duration) -> &mut Config {
        self.keepalive_config.interval = Some(keepalives_interval);
        self
    }

    /// Gets the time interval between TCP keepalive probes.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_keepalives_interval(&self) -> Option<Duration> {
        self.keepalive_config.interval
    }

    /// Sets the maximum number of TCP keepalive probes that will be sent before dropping a connection.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn keepalives_retries(&mut self, keepalives_retries: u32) -> &mut Config {
        self.keepalive_config.retries = Some(keepalives_retries);
        self
    }

    /// Gets the maximum number of TCP keepalive probes that will be sent before dropping a connection.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn get_keepalives_retries(&self) -> Option<u32> {
        self.keepalive_config.retries
    }

    /// Sets the requirements of the session.
    ///
    /// This can be used to connect to the primary server in a clustered database rather than one of the read-only
    /// secondary servers. Defaults to `Any`.
    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.target_session_attrs = target_session_attrs;
        self
    }

    /// Gets the requirements of the session.
    pub fn get_target_session_attrs(&self) -> TargetSessionAttrs {
        self.target_session_attrs
    }

    /// Sets the channel binding behavior.
    ///
    /// Defaults to `prefer`.
    pub fn channel_binding(&mut self, channel_binding: ChannelBinding) -> &mut Config {
        self.channel_binding = channel_binding;
        self
    }

    /// Gets the channel binding behavior.
    pub fn get_channel_binding(&self) -> ChannelBinding {
        self.channel_binding
    }

    /// Sets the host load balancing behavior.
    ///
    /// Defaults to `disable`.
    pub fn load_balance_hosts(&mut self, load_balance_hosts: LoadBalanceHosts) -> &mut Config {
        self.load_balance_hosts = load_balance_hosts;
        self
    }

    /// Gets the host load balancing behavior.
    pub fn get_load_balance_hosts(&self) -> LoadBalanceHosts {
        self.load_balance_hosts
    }

    fn param(&mut self, key: &str, value: &str) -> Result<(), Error> {
        match key {
            "user" => {
                self.user(value);
            }
            "password" => {
                self.password(value);
            }
            "dbname" => {
                self.dbname(value);
            }
            "options" => {
                self.options(value);
            }
            "application_name" => {
                self.application_name(value);
            }
            "sslmode" => {
                let mode = match value {
                    "disable" => SslMode::Disable,
                    "prefer" => SslMode::Prefer,
                    "require" => SslMode::Require,
                    _ => return Err(Error::config_parse(Box::new(InvalidValue("sslmode")))),
                };
                self.ssl_mode(mode);
            }
            "sslnegotiation" => {
                let mode = match value {
                    "postgres" => SslNegotiation::Postgres,
                    "direct" => SslNegotiation::Direct,
                    _ => {
                        return Err(Error::config_parse(Box::new(InvalidValue(
                            "sslnegotiation",
                        ))))
                    }
                };
                self.ssl_negotiation(mode);
            }
            "host" => {
                for host in value.split(',') {
                    self.host(host);
                }
            }
            "hostaddr" => {
                for hostaddr in value.split(',') {
                    let addr = hostaddr
                        .parse()
                        .map_err(|_| Error::config_parse(Box::new(InvalidValue("hostaddr"))))?;
                    self.hostaddr(addr);
                }
            }
            "port" => {
                for port in value.split(',') {
                    let port = if port.is_empty() {
                        5432
                    } else {
                        port.parse()
                            .map_err(|_| Error::config_parse(Box::new(InvalidValue("port"))))?
                    };
                    self.port(port);
                }
            }
            "connect_timeout" => {
                let timeout = value
                    .parse::<i64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("connect_timeout"))))?;
                if timeout > 0 {
                    self.connect_timeout(Duration::from_secs(timeout as u64));
                }
            }
            "tcp_user_timeout" => {
                let timeout = value
                    .parse::<i64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("tcp_user_timeout"))))?;
                if timeout > 0 {
                    self.tcp_user_timeout(Duration::from_secs(timeout as u64));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            "keepalives" => {
                let keepalives = value
                    .parse::<u64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("keepalives"))))?;
                self.keepalives(keepalives != 0);
            }
            #[cfg(not(target_arch = "wasm32"))]
            "keepalives_idle" => {
                let keepalives_idle = value
                    .parse::<i64>()
                    .map_err(|_| Error::config_parse(Box::new(InvalidValue("keepalives_idle"))))?;
                if keepalives_idle > 0 {
                    self.keepalives_idle(Duration::from_secs(keepalives_idle as u64));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            "keepalives_interval" => {
                let keepalives_interval = value.parse::<i64>().map_err(|_| {
                    Error::config_parse(Box::new(InvalidValue("keepalives_interval")))
                })?;
                if keepalives_interval > 0 {
                    self.keepalives_interval(Duration::from_secs(keepalives_interval as u64));
                }
            }
            #[cfg(not(target_arch = "wasm32"))]
            "keepalives_retries" => {
                let keepalives_retries = value.parse::<u32>().map_err(|_| {
                    Error::config_parse(Box::new(InvalidValue("keepalives_retries")))
                })?;
                self.keepalives_retries(keepalives_retries);
            }
            "target_session_attrs" => {
                let target_session_attrs = match value {
                    "any" => TargetSessionAttrs::Any,
                    "read-write" => TargetSessionAttrs::ReadWrite,
                    "read-only" => TargetSessionAttrs::ReadOnly,
                    _ => {
                        return Err(Error::config_parse(Box::new(InvalidValue(
                            "target_session_attrs",
                        ))));
                    }
                };
                self.target_session_attrs(target_session_attrs);
            }
            "channel_binding" => {
                let channel_binding = match value {
                    "disable" => ChannelBinding::Disable,
                    "prefer" => ChannelBinding::Prefer,
                    "require" => ChannelBinding::Require,
                    _ => {
                        return Err(Error::config_parse(Box::new(InvalidValue(
                            "channel_binding",
                        ))))
                    }
                };
                self.channel_binding(channel_binding);
            }
            "load_balance_hosts" => {
                let load_balance_hosts = match value {
                    "disable" => LoadBalanceHosts::Disable,
                    "random" => LoadBalanceHosts::Random,
                    _ => {
                        return Err(Error::config_parse(Box::new(InvalidValue(
                            "load_balance_hosts",
                        ))))
                    }
                };
                self.load_balance_hosts(load_balance_hosts);
            }
            key => {
                return Err(Error::config_parse(Box::new(UnknownOption(
                    key.to_string(),
                ))));
            }
        }

        Ok(())
    }

    /// Opens a connection to a PostgreSQL database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub async fn connect<T>(&self, tls: T) -> Result<(Client, Connection<Socket, T::Stream>), Error>
    where
        T: MakeTlsConnect<Socket>,
    {
        connect(tls, self).await
    }

    /// Connects to a PostgreSQL database over an arbitrary stream.
    ///
    /// All of the settings other than `user`, `password`, `dbname`, `options`, and `application_name` name are ignored.
    pub async fn connect_raw<S, T>(
        &self,
        stream: S,
        tls: T,
    ) -> Result<(Client, Connection<S, T::Stream>), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        connect_raw(stream, tls, true, self).await
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        match UrlParser::parse(s)? {
            Some(config) => Ok(config),
            None => Parser::parse(s),
        }
    }
}

// Omit password from debug output
impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redaction {}
        impl fmt::Debug for Redaction {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "_")
            }
        }

        let mut config_dbg = &mut f.debug_struct("Config");
        config_dbg = config_dbg
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| Redaction {}))
            .field("dbname", &self.dbname)
            .field("options", &self.options)
            .field("application_name", &self.application_name)
            .field("ssl_mode", &self.ssl_mode)
            .field("host", &self.host)
            .field("hostaddr", &self.hostaddr)
            .field("port", &self.port)
            .field("connect_timeout", &self.connect_timeout)
            .field("tcp_user_timeout", &self.tcp_user_timeout)
            .field("keepalives", &self.keepalives);

        #[cfg(not(target_arch = "wasm32"))]
        {
            config_dbg = config_dbg
                .field("keepalives_idle", &self.keepalive_config.idle)
                .field("keepalives_interval", &self.keepalive_config.interval)
                .field("keepalives_retries", &self.keepalive_config.retries);
        }

        config_dbg
            .field("target_session_attrs", &self.target_session_attrs)
            .field("channel_binding", &self.channel_binding)
            .field("load_balance_hosts", &self.load_balance_hosts)
            .finish()
    }
}

#[derive(Debug)]
struct UnknownOption(String);

impl fmt::Display for UnknownOption {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "unknown option `{}`", self.0)
    }
}

impl error::Error for UnknownOption {}

#[derive(Debug)]
struct InvalidValue(&'static str);

impl fmt::Display for InvalidValue {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "invalid value for option `{}`", self.0)
    }
}

impl error::Error for InvalidValue {}

struct Parser<'a> {
    s: &'a str,
    it: iter::Peekable<str::CharIndices<'a>>,
}

impl<'a> Parser<'a> {
    fn parse(s: &'a str) -> Result<Config, Error> {
        let mut parser = Parser {
            s,
            it: s.char_indices().peekable(),
        };

        let mut config = Config::new();

        while let Some((key, value)) = parser.parameter()? {
            config.param(key, &value)?;
        }

        Ok(config)
    }

    fn skip_ws(&mut self) {
        self.take_while(char::is_whitespace);
    }

    fn take_while<F>(&mut self, f: F) -> &'a str
    where
        F: Fn(char) -> bool,
    {
        let start = match self.it.peek() {
            Some(&(i, _)) => i,
            None => return "",
        };

        loop {
            match self.it.peek() {
                Some(&(_, c)) if f(c) => {
                    self.it.next();
                }
                Some(&(i, _)) => return &self.s[start..i],
                None => return &self.s[start..],
            }
        }
    }

    fn eat(&mut self, target: char) -> Result<(), Error> {
        match self.it.next() {
            Some((_, c)) if c == target => Ok(()),
            Some((i, c)) => {
                let m =
                    format!("unexpected character at byte {i}: expected `{target}` but got `{c}`");
                Err(Error::config_parse(m.into()))
            }
            None => Err(Error::config_parse("unexpected EOF".into())),
        }
    }

    fn eat_if(&mut self, target: char) -> bool {
        match self.it.peek() {
            Some(&(_, c)) if c == target => {
                self.it.next();
                true
            }
            _ => false,
        }
    }

    fn keyword(&mut self) -> Option<&'a str> {
        let s = self.take_while(|c| match c {
            c if c.is_whitespace() => false,
            '=' => false,
            _ => true,
        });

        if s.is_empty() {
            None
        } else {
            Some(s)
        }
    }

    fn value(&mut self) -> Result<String, Error> {
        let value = if self.eat_if('\'') {
            let value = self.quoted_value()?;
            self.eat('\'')?;
            value
        } else {
            self.simple_value()?
        };

        Ok(value)
    }

    fn simple_value(&mut self) -> Result<String, Error> {
        let mut value = String::new();

        while let Some(&(_, c)) = self.it.peek() {
            if c.is_whitespace() {
                break;
            }

            self.it.next();
            if c == '\\' {
                if let Some((_, c2)) = self.it.next() {
                    value.push(c2);
                }
            } else {
                value.push(c);
            }
        }

        if value.is_empty() {
            return Err(Error::config_parse("unexpected EOF".into()));
        }

        Ok(value)
    }

    fn quoted_value(&mut self) -> Result<String, Error> {
        let mut value = String::new();

        while let Some(&(_, c)) = self.it.peek() {
            if c == '\'' {
                return Ok(value);
            }

            self.it.next();
            if c == '\\' {
                if let Some((_, c2)) = self.it.next() {
                    value.push(c2);
                }
            } else {
                value.push(c);
            }
        }

        Err(Error::config_parse(
            "unterminated quoted connection parameter value".into(),
        ))
    }

    fn parameter(&mut self) -> Result<Option<(&'a str, String)>, Error> {
        self.skip_ws();
        let keyword = match self.keyword() {
            Some(keyword) => keyword,
            None => return Ok(None),
        };
        self.skip_ws();
        self.eat('=')?;
        self.skip_ws();
        let value = self.value()?;

        Ok(Some((keyword, value)))
    }
}

// This is a pretty sloppy "URL" parser, but it matches the behavior of libpq, where things really aren't very strict
struct UrlParser<'a> {
    s: &'a str,
    config: Config,
}

impl<'a> UrlParser<'a> {
    fn parse(s: &'a str) -> Result<Option<Config>, Error> {
        let s = match Self::remove_url_prefix(s) {
            Some(s) => s,
            None => return Ok(None),
        };

        let mut parser = UrlParser {
            s,
            config: Config::new(),
        };

        parser.parse_credentials()?;
        parser.parse_host()?;
        parser.parse_path()?;
        parser.parse_params()?;

        Ok(Some(parser.config))
    }

    fn remove_url_prefix(s: &str) -> Option<&str> {
        for prefix in &["postgres://", "postgresql://"] {
            if let Some(stripped) = s.strip_prefix(prefix) {
                return Some(stripped);
            }
        }

        None
    }

    fn take_until(&mut self, end: &[char]) -> Option<&'a str> {
        match self.s.find(end) {
            Some(pos) => {
                let (head, tail) = self.s.split_at(pos);
                self.s = tail;
                Some(head)
            }
            None => None,
        }
    }

    fn take_all(&mut self) -> &'a str {
        mem::take(&mut self.s)
    }

    fn eat_byte(&mut self) {
        self.s = &self.s[1..];
    }

    fn parse_credentials(&mut self) -> Result<(), Error> {
        let creds = match self.take_until(&['@']) {
            Some(creds) => creds,
            None => return Ok(()),
        };
        self.eat_byte();

        let mut it = creds.splitn(2, ':');
        let user = self.decode(it.next().unwrap())?;
        self.config.user(user);

        if let Some(password) = it.next() {
            let password = Cow::from(percent_encoding::percent_decode(password.as_bytes()));
            self.config.password(password);
        }

        Ok(())
    }

    fn parse_host(&mut self) -> Result<(), Error> {
        let host = match self.take_until(&['/', '?']) {
            Some(host) => host,
            None => self.take_all(),
        };

        if host.is_empty() {
            return Ok(());
        }

        for chunk in host.split(',') {
            let (host, port) = if chunk.starts_with('[') {
                let idx = match chunk.find(']') {
                    Some(idx) => idx,
                    None => return Err(Error::config_parse(InvalidValue("host").into())),
                };

                let host = &chunk[1..idx];
                let remaining = &chunk[idx + 1..];
                let port = if let Some(port) = remaining.strip_prefix(':') {
                    Some(port)
                } else if remaining.is_empty() {
                    None
                } else {
                    return Err(Error::config_parse(InvalidValue("host").into()));
                };

                (host, port)
            } else {
                let mut it = chunk.splitn(2, ':');
                (it.next().unwrap(), it.next())
            };

            self.host_param(host)?;
            let port = self.decode(port.unwrap_or("5432"))?;
            self.config.param("port", &port)?;
        }

        Ok(())
    }

    fn parse_path(&mut self) -> Result<(), Error> {
        if !self.s.starts_with('/') {
            return Ok(());
        }
        self.eat_byte();

        let dbname = match self.take_until(&['?']) {
            Some(dbname) => dbname,
            None => self.take_all(),
        };

        if !dbname.is_empty() {
            self.config.dbname(self.decode(dbname)?);
        }

        Ok(())
    }

    fn parse_params(&mut self) -> Result<(), Error> {
        if !self.s.starts_with('?') {
            return Ok(());
        }
        self.eat_byte();

        while !self.s.is_empty() {
            let key = match self.take_until(&['=']) {
                Some(key) => self.decode(key)?,
                None => return Err(Error::config_parse("unterminated parameter".into())),
            };
            self.eat_byte();

            let value = match self.take_until(&['&']) {
                Some(value) => {
                    self.eat_byte();
                    value
                }
                None => self.take_all(),
            };

            if key == "host" {
                self.host_param(value)?;
            } else {
                let value = self.decode(value)?;
                self.config.param(&key, &value)?;
            }
        }

        Ok(())
    }

    #[cfg(unix)]
    fn host_param(&mut self, s: &str) -> Result<(), Error> {
        let decoded = Cow::from(percent_encoding::percent_decode(s.as_bytes()));
        if decoded.first() == Some(&b'/') {
            self.config.host_path(OsStr::from_bytes(&decoded));
        } else {
            let decoded = str::from_utf8(&decoded).map_err(|e| Error::config_parse(Box::new(e)))?;
            self.config.host(decoded);
        }

        Ok(())
    }

    #[cfg(not(unix))]
    fn host_param(&mut self, s: &str) -> Result<(), Error> {
        let s = self.decode(s)?;
        self.config.param("host", &s)
    }

    fn decode(&self, s: &'a str) -> Result<Cow<'a, str>, Error> {
        percent_encoding::percent_decode(s.as_bytes())
            .decode_utf8()
            .map_err(|e| Error::config_parse(e.into()))
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use crate::{config::Host, Config};

    #[test]
    fn test_simple_parsing() {
        let s = "user=pass_user dbname=postgres host=host1,host2 hostaddr=127.0.0.1,127.0.0.2 port=26257";
        let config = s.parse::<Config>().unwrap();
        assert_eq!(Some("pass_user"), config.get_user());
        assert_eq!(Some("postgres"), config.get_dbname());
        assert_eq!(
            [
                Host::Tcp("host1".to_string()),
                Host::Tcp("host2".to_string())
            ],
            config.get_hosts(),
        );

        assert_eq!(
            [
                "127.0.0.1".parse::<IpAddr>().unwrap(),
                "127.0.0.2".parse::<IpAddr>().unwrap()
            ],
            config.get_hostaddrs(),
        );

        assert_eq!(1, 1);
    }

    #[test]
    fn test_invalid_hostaddr_parsing() {
        let s = "user=pass_user dbname=postgres host=host1 hostaddr=127.0.0 port=26257";
        s.parse::<Config>().err().unwrap();
    }
}
