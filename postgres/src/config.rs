//! Connection configuration.

#![allow(clippy::doc_overindented_list_items)]

use crate::connection::Connection;
use crate::Client;
use log::info;
use std::fmt;
use std::net::IpAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime;
#[doc(inline)]
pub use tokio_postgres::config::{
    ChannelBinding, Host, LoadBalanceHosts, SslMode, SslNegotiation, TargetSessionAttrs,
};
use tokio_postgres::error::DbError;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::{Error, Socket};

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
/// * `sslnegotiation` - TLS negotiation method. If set to `direct`, the client will perform direct TLS handshake, this only works for PostgreSQL 17 and newer.
///     Note that you will need to setup ALPN of TLS client configuration to `postgresql` when using direct TLS.
///     If set to `postgres`, the default value, it follows original postgres wire protocol to perform the negotiation.
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
#[derive(Clone)]
pub struct Config {
    config: tokio_postgres::Config,
    notice_callback: Arc<dyn Fn(DbError) + Send + Sync>,
}

impl fmt::Debug for Config {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Config")
            .field("config", &self.config)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Config {
        Config::new()
    }
}

impl Config {
    /// Creates a new configuration.
    pub fn new() -> Config {
        tokio_postgres::Config::new().into()
    }

    /// Sets the user to authenticate with.
    ///
    /// If the user is not set, then this defaults to the user executing this process.
    pub fn user(&mut self, user: &str) -> &mut Config {
        self.config.user(user);
        self
    }

    /// Gets the user to authenticate with, if one has been configured with
    /// the `user` method.
    pub fn get_user(&self) -> Option<&str> {
        self.config.get_user()
    }

    /// Sets the password to authenticate with.
    pub fn password<T>(&mut self, password: T) -> &mut Config
    where
        T: AsRef<[u8]>,
    {
        self.config.password(password);
        self
    }

    /// Gets the password to authenticate with, if one has been configured with
    /// the `password` method.
    pub fn get_password(&self) -> Option<&[u8]> {
        self.config.get_password()
    }

    /// Sets the name of the database to connect to.
    ///
    /// Defaults to the user.
    pub fn dbname(&mut self, dbname: &str) -> &mut Config {
        self.config.dbname(dbname);
        self
    }

    /// Gets the name of the database to connect to, if one has been configured
    /// with the `dbname` method.
    pub fn get_dbname(&self) -> Option<&str> {
        self.config.get_dbname()
    }

    /// Sets command line options used to configure the server.
    pub fn options(&mut self, options: &str) -> &mut Config {
        self.config.options(options);
        self
    }

    /// Gets the command line options used to configure the server, if the
    /// options have been set with the `options` method.
    pub fn get_options(&self) -> Option<&str> {
        self.config.get_options()
    }

    /// Sets the value of the `application_name` runtime parameter.
    pub fn application_name(&mut self, application_name: &str) -> &mut Config {
        self.config.application_name(application_name);
        self
    }

    /// Gets the value of the `application_name` runtime parameter, if it has
    /// been set with the `application_name` method.
    pub fn get_application_name(&self) -> Option<&str> {
        self.config.get_application_name()
    }

    /// Sets the SSL configuration.
    ///
    /// Defaults to `prefer`.
    pub fn ssl_mode(&mut self, ssl_mode: SslMode) -> &mut Config {
        self.config.ssl_mode(ssl_mode);
        self
    }

    /// Gets the SSL configuration.
    pub fn get_ssl_mode(&self) -> SslMode {
        self.config.get_ssl_mode()
    }

    /// Sets the SSL negotiation method
    pub fn ssl_negotiation(&mut self, ssl_negotiation: SslNegotiation) -> &mut Config {
        self.config.ssl_negotiation(ssl_negotiation);
        self
    }

    /// Gets the SSL negotiation method
    pub fn get_ssl_negotiation(&self) -> SslNegotiation {
        self.config.get_ssl_negotiation()
    }

    /// Adds a host to the configuration.
    ///
    /// Multiple hosts can be specified by calling this method multiple times, and each will be tried in order. On Unix
    /// systems, a host starting with a `/` is interpreted as a path to a directory containing Unix domain sockets.
    /// There must be either no hosts, or the same number of hosts as hostaddrs.
    pub fn host(&mut self, host: &str) -> &mut Config {
        self.config.host(host);
        self
    }

    /// Gets the hosts that have been added to the configuration with `host`.
    pub fn get_hosts(&self) -> &[Host] {
        self.config.get_hosts()
    }

    /// Gets the hostaddrs that have been added to the configuration with `hostaddr`.
    pub fn get_hostaddrs(&self) -> &[IpAddr] {
        self.config.get_hostaddrs()
    }

    /// Adds a Unix socket host to the configuration.
    ///
    /// Unlike `host`, this method allows non-UTF8 paths.
    #[cfg(unix)]
    pub fn host_path<T>(&mut self, host: T) -> &mut Config
    where
        T: AsRef<Path>,
    {
        self.config.host_path(host);
        self
    }

    /// Adds a hostaddr to the configuration.
    ///
    /// Multiple hostaddrs can be specified by calling this method multiple times, and each will be tried in order.
    /// There must be either no hostaddrs, or the same number of hostaddrs as hosts.
    pub fn hostaddr(&mut self, hostaddr: IpAddr) -> &mut Config {
        self.config.hostaddr(hostaddr);
        self
    }

    /// Adds a port to the configuration.
    ///
    /// Multiple ports can be specified by calling this method multiple times. There must either be no ports, in which
    /// case the default of 5432 is used, a single port, in which it is used for all hosts, or the same number of ports
    /// as hosts.
    pub fn port(&mut self, port: u16) -> &mut Config {
        self.config.port(port);
        self
    }

    /// Gets the ports that have been added to the configuration with `port`.
    pub fn get_ports(&self) -> &[u16] {
        self.config.get_ports()
    }

    /// Sets the timeout applied to socket-level connection attempts.
    ///
    /// Note that hostnames can resolve to multiple IP addresses, and this timeout will apply to each address of each
    /// host separately. Defaults to no limit.
    pub fn connect_timeout(&mut self, connect_timeout: Duration) -> &mut Config {
        self.config.connect_timeout(connect_timeout);
        self
    }

    /// Gets the connection timeout, if one has been set with the
    /// `connect_timeout` method.
    pub fn get_connect_timeout(&self) -> Option<&Duration> {
        self.config.get_connect_timeout()
    }

    /// Sets the TCP user timeout.
    ///
    /// This is ignored for Unix domain socket connections. It is only supported on systems where
    /// TCP_USER_TIMEOUT is available and will default to the system default if omitted or set to 0;
    /// on other systems, it has no effect.
    pub fn tcp_user_timeout(&mut self, tcp_user_timeout: Duration) -> &mut Config {
        self.config.tcp_user_timeout(tcp_user_timeout);
        self
    }

    /// Gets the TCP user timeout, if one has been set with the
    /// `user_timeout` method.
    pub fn get_tcp_user_timeout(&self) -> Option<&Duration> {
        self.config.get_tcp_user_timeout()
    }

    /// Controls the use of TCP keepalive.
    ///
    /// This is ignored for Unix domain socket connections. Defaults to `true`.
    pub fn keepalives(&mut self, keepalives: bool) -> &mut Config {
        self.config.keepalives(keepalives);
        self
    }

    /// Reports whether TCP keepalives will be used.
    pub fn get_keepalives(&self) -> bool {
        self.config.get_keepalives()
    }

    /// Sets the amount of idle time before a keepalive packet is sent on the connection.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled. Defaults to 2 hours.
    pub fn keepalives_idle(&mut self, keepalives_idle: Duration) -> &mut Config {
        self.config.keepalives_idle(keepalives_idle);
        self
    }

    /// Gets the configured amount of idle time before a keepalive packet will
    /// be sent on the connection.
    pub fn get_keepalives_idle(&self) -> Duration {
        self.config.get_keepalives_idle()
    }

    /// Sets the time interval between TCP keepalive probes.
    /// On Windows, this sets the value of the tcp_keepalive struct’s keepaliveinterval field.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled.
    pub fn keepalives_interval(&mut self, keepalives_interval: Duration) -> &mut Config {
        self.config.keepalives_interval(keepalives_interval);
        self
    }

    /// Gets the time interval between TCP keepalive probes.
    pub fn get_keepalives_interval(&self) -> Option<Duration> {
        self.config.get_keepalives_interval()
    }

    /// Sets the maximum number of TCP keepalive probes that will be sent before dropping a connection.
    ///
    /// This is ignored for Unix domain sockets, or if the `keepalives` option is disabled.
    pub fn keepalives_retries(&mut self, keepalives_retries: u32) -> &mut Config {
        self.config.keepalives_retries(keepalives_retries);
        self
    }

    /// Gets the maximum number of TCP keepalive probes that will be sent before dropping a connection.
    pub fn get_keepalives_retries(&self) -> Option<u32> {
        self.config.get_keepalives_retries()
    }

    /// Sets the requirements of the session.
    ///
    /// This can be used to connect to the primary server in a clustered database rather than one of the read-only
    /// secondary servers. Defaults to `Any`.
    pub fn target_session_attrs(
        &mut self,
        target_session_attrs: TargetSessionAttrs,
    ) -> &mut Config {
        self.config.target_session_attrs(target_session_attrs);
        self
    }

    /// Gets the requirements of the session.
    pub fn get_target_session_attrs(&self) -> TargetSessionAttrs {
        self.config.get_target_session_attrs()
    }

    /// Sets the channel binding behavior.
    ///
    /// Defaults to `prefer`.
    pub fn channel_binding(&mut self, channel_binding: ChannelBinding) -> &mut Config {
        self.config.channel_binding(channel_binding);
        self
    }

    /// Gets the channel binding behavior.
    pub fn get_channel_binding(&self) -> ChannelBinding {
        self.config.get_channel_binding()
    }

    /// Sets the host load balancing behavior.
    ///
    /// Defaults to `disable`.
    pub fn load_balance_hosts(&mut self, load_balance_hosts: LoadBalanceHosts) -> &mut Config {
        self.config.load_balance_hosts(load_balance_hosts);
        self
    }

    /// Gets the host load balancing behavior.
    pub fn get_load_balance_hosts(&self) -> LoadBalanceHosts {
        self.config.get_load_balance_hosts()
    }

    /// Sets the notice callback.
    ///
    /// This callback will be invoked with the contents of every
    /// [`AsyncMessage::Notice`] that is received by the connection. Notices use
    /// the same structure as errors, but they are not "errors" per-se.
    ///
    /// Notices are distinct from notifications, which are instead accessible
    /// via the [`Notifications`] API.
    ///
    /// [`AsyncMessage::Notice`]: tokio_postgres::AsyncMessage::Notice
    /// [`Notifications`]: crate::Notifications
    pub fn notice_callback<F>(&mut self, f: F) -> &mut Config
    where
        F: Fn(DbError) + Send + Sync + 'static,
    {
        self.notice_callback = Arc::new(f);
        self
    }

    /// Opens a connection to a PostgreSQL database.
    pub fn connect<T>(&self, tls: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        let runtime = runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(); // FIXME don't unwrap

        let (client, connection) = runtime.block_on(self.config.connect(tls))?;

        let connection = Connection::new(runtime, connection, self.notice_callback.clone());
        Ok(Client::new(connection, client))
    }
}

impl FromStr for Config {
    type Err = Error;

    fn from_str(s: &str) -> Result<Config, Error> {
        s.parse::<tokio_postgres::Config>().map(Config::from)
    }
}

impl From<tokio_postgres::Config> for Config {
    fn from(config: tokio_postgres::Config) -> Config {
        Config {
            config,
            notice_callback: Arc::new(|notice| {
                info!("{}: {}", notice.severity(), notice.message())
            }),
        }
    }
}
