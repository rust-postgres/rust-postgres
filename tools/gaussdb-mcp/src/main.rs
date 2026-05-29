mod queries;
mod server;

use keyring::Entry;
use rmcp::{transport::stdio, ServiceExt};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use crate::server::format_error_chain;

const KEYRING_SERVICE: &str = "gaussdb-mcp";
const KEYRING_SENTINEL: &str = "keyring";

struct VerboseDetails {
    server_version: Option<String>,
    server_version_num: Option<String>,
    protocol_version: Option<String>,
    current_user: Option<String>,
    current_database: Option<String>,
    server_addr: Option<String>,
    server_port: Option<String>,
    start_time: Option<String>,
    is_in_recovery: Option<bool>,
    ssl_is_used: Option<bool>,
    ssl_version: Option<String>,
    ssl_cipher: Option<String>,
    elapsed: Duration,
    guc_max_connections: Option<String>,
    guc_shared_buffers: Option<String>,
    guc_work_mem: Option<String>,
    guc_timezone: Option<String>,
    guc_data_directory: Option<String>,
}

struct TlsCertInfo {
    subject: String,
    issuer: String,
    valid_from: String,
    valid_to: String,
    serial: String,
}

#[derive(Debug, Deserialize)]
struct Config {
    url: Option<String>,
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
    dbname: Option<String>,
    sslmode: Option<String>,
}

struct ResolvedConfig {
    connection_url: String,
    config_path: Option<PathBuf>,
    plaintext_password: Option<String>,
    keyring_username: String,
    password_source: PasswordSource,
}

#[derive(Clone, Copy)]
enum PasswordSource {
    EnvVar,
    Plaintext,
    Keyring,
    None,
}

enum LazyResolvedConfig {
    Ready(ResolvedConfig),
    Pending(Arc<dyn (Fn() -> Result<String, String>) + Send + Sync>),
}

impl Config {
    fn keyring_username(&self) -> String {
        match (&self.user, &self.host, &self.dbname) {
            (Some(u), Some(h), Some(d)) => format!("{}@{}/{}", u, h, d),
            (Some(u), Some(h), None) => format!("{}@{}", u, h),
            (Some(u), _, _) => u.clone(),
            _ => "default".to_string(),
        }
    }

    fn to_connection_url(&self) -> Option<String> {
        if let Some(ref url) = self.url {
            return Some(url.clone());
        }

        if self.host.is_none() && self.user.is_none() {
            return None;
        }

        let mut parts = Vec::new();
        if let Some(ref host) = self.host {
            parts.push(format!("host={}", host));
        }
        if let Some(port) = self.port {
            parts.push(format!("port={}", port));
        }
        if let Some(ref user) = self.user {
            parts.push(format!("user={}", user));
        }
        if let Some(ref password) = self.password {
            parts.push(format!("password={}", password));
        }
        if let Some(ref dbname) = self.dbname {
            parts.push(format!("dbname={}", dbname));
        }
        if let Some(ref sslmode) = self.sslmode {
            parts.push(format!("sslmode={}", sslmode));
        }

        Some(parts.join(" "))
    }
}

fn read_keyring_password(username: &str) -> Result<String, String> {
    let entry = Entry::new(KEYRING_SERVICE, username)
        .map_err(|e| format!("keyring entry creation failed: {}", e))?;
    entry
        .get_password()
        .map_err(|e| format!(
            "keyring password not found for '{}'. Store it first:\n  \
             gaussdb-mcp --store-password <password> --config <path>\n  \
             or set password in config file as plaintext (will be migrated automatically).\n  \
             Keyring error: {}", username, e
        ))
}

fn store_keyring_password(username: &str, password: &str) -> Result<(), String> {
    let entry = Entry::new(KEYRING_SERVICE, username)
        .map_err(|e| format!("keyring entry creation failed: {}", e))?;
    entry
        .set_password(password)
        .map_err(|e| format!("keyring store failed: {}", e))?;
    // Verify the password is actually readable back
    let verified = entry
        .get_password()
        .map_err(|e| format!("keyring verification failed (password was stored but cannot be read back): {}", e))?;
    if verified != password {
        return Err("keyring verification failed: read-back mismatch".to_string());
    }
    Ok(())
}

fn rewrite_password_to_sentinel(path: &std::path::Path) -> std::io::Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut new_content = String::new();
    let mut replaced = false;

    for line in content.lines() {
        if !replaced && line.trim().starts_with("password") {
            if line.contains('=') {
                let indent = &line[..line.find("password").unwrap_or(0)];
                new_content.push_str(&format!("{}password = \"{}\"", indent, KEYRING_SENTINEL));
                replaced = true;
            } else {
                new_content.push_str(line);
            }
        } else {
            new_content.push_str(line);
        }
        new_content.push('\n');
    }

    if content.ends_with('\n') && new_content.ends_with("\n\n") {
        new_content.pop();
    }

    std::fs::write(path, new_content)
}

fn default_config_path() -> Option<PathBuf> {
    dirs::home_dir().map(|p| p.join(".gaussdb-mcp.toml"))
}

fn handle_store_password() {
    let args: Vec<String> = std::env::args().collect();
    let mut password = None;
    let mut config_path = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--store-password" => {
                password = args.get(i + 1).cloned();
                i += 2;
            }
            "--config" => {
                config_path = args.get(i + 1).map(PathBuf::from);
                i += 2;
            }
            _ => i += 1,
        }
    }

    let password = password.unwrap_or_else(|| {
        eprintln!("error: --store-password requires a password argument");
        std::process::exit(1);
    });

    let config_path = config_path
        .or_else(default_config_path)
        .unwrap_or_else(|| {
            eprintln!("error: no config file specified and no default found");
            std::process::exit(1);
        });

    if !config_path.exists() {
        eprintln!("error: config file not found: {}", config_path.display());
        std::process::exit(1);
    }

    let content = std::fs::read_to_string(&config_path).unwrap_or_else(|e| {
        eprintln!("error: failed to read {}: {}", config_path.display(), e);
        std::process::exit(1);
    });

    let config: Config = toml::from_str(&content).unwrap_or_else(|e| {
        eprintln!("error: failed to parse {}: {}", config_path.display(), e);
        std::process::exit(1);
    });

    let keyring_user = config.keyring_username();

    if let Err(e) = store_keyring_password(&keyring_user, &password) {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }

    println!("Password stored in OS keychain for '{}'.", keyring_user);

    if config.password.as_ref().is_some_and(|p| p != KEYRING_SENTINEL) {
        if let Err(e) = rewrite_password_to_sentinel(&config_path) {
            eprintln!("warning: failed to update config file: {}", e);
        } else {
            println!(
                "Config file {} updated: password = \"{}\"",
                config_path.display(),
                KEYRING_SENTINEL
            );
        }
    }
}

fn resolve_connection_url_or_check() -> ResolvedConfig {
    resolve_connection_url_inner(false).unwrap_or_else(|e| {
        eprintln!("error: {}", e);
        std::process::exit(1);
    })
}

fn resolve_connection_url_inner(_allow_keyring_diag: bool) -> Result<ResolvedConfig, String> {
    // 1. Environment variable (highest priority)
    if let Ok(url) = std::env::var("GAUSSDB_URL").or_else(|_| std::env::var("DATABASE_URL")) {
        return Ok(ResolvedConfig {
            connection_url: url,
            config_path: None,
            plaintext_password: None,
            keyring_username: String::new(),
            password_source: PasswordSource::EnvVar,
        });
    }

    // 2. --config <path> CLI argument
    let args: Vec<String> = std::env::args().collect();
    let config_path = if let Some(pos) = args.iter().position(|a| a == "--config") {
        args.get(pos + 1)
            .map(PathBuf::from)
            .ok_or_else(|| "--config requires a file path argument".to_string())?
    } else {
        // 3. Default config file ~/.gaussdb-mcp.toml
        match default_config_path() {
            Some(p) if p.exists() => p,
            _ => {
                return Err(
                    "No connection configuration found. Use one of:\n\
                     \n\
                     \u{20} 1. Set GAUSSDB_URL or DATABASE_URL environment variable\n\
                     \u{20}    export GAUSSDB_URL=\"host=localhost user=postgres password=secret dbname=mydb\"\n\
                     \n\
                     \u{20} 2. Create ~/.gaussdb-mcp.toml config file:\n\
                     \u{20}    host = \"localhost\"\n\
                     \u{20}    user = \"postgres\"\n\
                     \u{20}    password = \"secret\"\n\
                     \u{20}    dbname = \"mydb\"\n\
                     \n\
                     \u{20} 3. Pass --config <path> to specify a config file\n\
                     \n\
                     \u{20} Password will be migrated to OS keychain on first successful connection."
                        .to_string(),
                );
            }
        }
    };

    let content = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("failed to read config file {}: {}", config_path.display(), e))?;

    let mut config: Config = toml::from_str(&content)
        .map_err(|e| format!("failed to parse config file {}: {}", config_path.display(), e))?;

    let keyring_user = config.keyring_username();
    let is_sentinel = config.password.as_deref() == Some(KEYRING_SENTINEL);
    let is_plaintext = config
        .password
        .as_ref()
        .is_some_and(|p| p != KEYRING_SENTINEL);
    let has_no_password = config.password.is_none();

    let password_source = if is_plaintext {
        PasswordSource::Plaintext
    } else if is_sentinel {
        PasswordSource::Keyring
    } else if has_no_password {
        PasswordSource::None
    } else {
        PasswordSource::None
    };

    if is_sentinel || has_no_password {
        let pw = read_keyring_password(&keyring_user)?;
        config.password = Some(pw);
    }

    let plaintext_password = if is_plaintext {
        config.password.clone()
    } else {
        None
    };

    let connection_url = config.to_connection_url().ok_or_else(|| {
        format!(
            "config file {} must contain either `url` or at least `host`/`user` fields",
            config_path.display()
        )
    })?;

    Ok(ResolvedConfig {
        connection_url,
        config_path: Some(config_path),
        plaintext_password,
        keyring_username: keyring_user,
        password_source,
    })
}

fn resolve_connection_url_lazy() -> LazyResolvedConfig {
    resolve_connection_url_lazy_inner().unwrap_or_else(|e| {
        eprintln!("error: {}", e);
        std::process::exit(1);
    })
}

fn resolve_connection_url_lazy_inner() -> Result<LazyResolvedConfig, String> {
    // 1. Environment variable (highest priority) - no keychain needed
    if let Ok(url) = std::env::var("GAUSSDB_URL").or_else(|_| std::env::var("DATABASE_URL")) {
        return Ok(LazyResolvedConfig::Ready(ResolvedConfig {
            connection_url: url,
            config_path: None,
            plaintext_password: None,
            keyring_username: String::new(),
            password_source: PasswordSource::EnvVar,
        }));
    }

    // 2. --config <path> CLI argument
    let args: Vec<String> = std::env::args().collect();
    let config_path = if let Some(pos) = args.iter().position(|a| a == "--config") {
        args.get(pos + 1)
            .map(PathBuf::from)
            .ok_or_else(|| "--config requires a file path argument".to_string())?
    } else {
        // 3. Default config file ~/.gaussdb-mcp.toml
        match default_config_path() {
            Some(p) if p.exists() => p,
            _ => {
                return Err(
                    "No connection configuration found. Use one of:\n\
                     \n\
                     \u{20} 1. Set GAUSSDB_URL or DATABASE_URL environment variable\n\
                     \u{20}    export GAUSSDB_URL=\"host=localhost user=postgres password=secret dbname=mydb\"\n\
                     \n\
                     \u{20} 2. Create ~/.gaussdb-mcp.toml config file:\n\
                     \u{20}    host = \"localhost\"\n\
                     \u{20}    user = \"postgres\"\n\
                     \u{20}    password = \"secret\"\n\
                     \u{20}    dbname = \"mydb\"\n\
                     \n\
                     \u{20} 3. Pass --config <path> to specify a config file\n\
                     \n\
                     \u{20} Password will be migrated to OS keychain on first successful connection."
                        .to_string(),
                );
            }
        }
    };

    let content = std::fs::read_to_string(&config_path)
        .map_err(|e| format!("failed to read config file {}: {}", config_path.display(), e))?;

    let config: Config = toml::from_str(&content)
        .map_err(|e| format!("failed to parse config file {}: {}", config_path.display(), e))?;

    let keyring_user = config.keyring_username();

    if let Some(ref url) = config.url {
        return Ok(LazyResolvedConfig::Ready(ResolvedConfig {
            connection_url: url.clone(),
            config_path: Some(config_path),
            plaintext_password: None,
            keyring_username: keyring_user,
            password_source: PasswordSource::Plaintext,
        }));
    }

    let is_sentinel = config.password.as_deref() == Some(KEYRING_SENTINEL);
    let is_plaintext = config.password
        .as_ref()
        .is_some_and(|p| p != KEYRING_SENTINEL);

    if is_plaintext {
        let plaintext_password = config.password.clone();
        let connection_url = config.to_connection_url().ok_or_else(|| {
            format!(
                "config file {} must contain either `url` or at least `host`/`user` fields",
                config_path.display()
            )
        })?;
        Ok(LazyResolvedConfig::Ready(ResolvedConfig {
            connection_url,
            config_path: Some(config_path),
            plaintext_password,
            keyring_username: keyring_user,
            password_source: PasswordSource::Plaintext,
        }))
    } else {
        let password_source = if is_sentinel {
            PasswordSource::Keyring
        } else {
            PasswordSource::None
        };

        let host = config.host.clone();
        let port = config.port;
        let user = config.user.clone();
        let dbname = config.dbname.clone();
        let sslmode = config.sslmode.clone();

        if host.is_none() && user.is_none() {
            return Err(format!(
                "config file {} must contain either `url` or at least `host`/`user` fields",
                config_path.display()
            ));
        }

        let resolver = Arc::new(move || {
            let password = match password_source {
                PasswordSource::Keyring => Some(read_keyring_password(&keyring_user)?),
                PasswordSource::None => None,
                _ => unreachable!(),
            };

            let mut parts = Vec::new();
            if let Some(ref h) = host {
                parts.push(format!("host={}", h));
            }
            if let Some(p) = port {
                parts.push(format!("port={}", p));
            }
            if let Some(ref u) = user {
                parts.push(format!("user={}", u));
            }
            if let Some(pw) = password {
                parts.push(format!("password={}", pw));
            }
            if let Some(ref d) = dbname {
                parts.push(format!("dbname={}", d));
            }
            if let Some(ref s) = sslmode {
                parts.push(format!("sslmode={}", s));
            }

            Ok(parts.join(" "))
        });

        info!("keychain read deferred until first tool invocation");
        Ok(LazyResolvedConfig::Pending(resolver))
    }
}

fn init_logging() {
    let log_dir = dirs::data_local_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("gaussdb-mcp");

    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        eprintln!("warning: cannot create log dir {}: {}", log_dir.display(), e);
    }

    let file_appender = tracing_appender::rolling::daily(&log_dir, "gaussdb-mcp.log");
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("gaussdb_mcp=info"));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(file_appender)
        .with_ansi(false)
        .with_target(false)
        .init();

    info!("log file: {}/gaussdb-mcp.log", log_dir.display());
}

async fn handle_check_connection(resolved: &ResolvedConfig, verbose: bool) {
    use server::redact_url;

    let url = &resolved.connection_url;
    let redacted = redact_url(url);

    // ── Phase 1: Keyring check ──
    match resolved.password_source {
        PasswordSource::Keyring => {
            eprintln!("[Keyring] Password read from OS keychain (user: {})", resolved.keyring_username);
            let entry_result = Entry::new(KEYRING_SERVICE, &resolved.keyring_username)
                .and_then(|e| e.get_password());
            match entry_result {
                Ok(pw) => {
                    if pw.is_empty() {
                        eprintln!("  ⚠ WARNING: keyring returned empty password");
                    } else {
                        eprintln!("  ✓ Keyring accessible, password retrieved ({} chars)", pw.len());
                    }
                }
                Err(e) => {
                    eprintln!("  ✗ Keyring read-back failed: {}", e);
                    eprintln!("    This means the password was already read once but keyring may be unreliable.");
                    eprintln!("    Consider changing password in config from \"keyring\" back to plaintext.");
                }
            }
            eprintln!();
        }
        PasswordSource::Plaintext => {
            eprintln!("[Keyring] Password from config file (plaintext)");
            match check_keyring_available(&resolved.keyring_username) {
                Ok(()) => {
                    eprintln!("  ✓ OS keychain is available — password will be migrated on first MCP connection");
                }
                Err(e) => {
                    eprintln!("  ⚠ OS keychain NOT available: {}", e);
                    eprintln!("    Plaintext password will be kept in config file (no migration).");
                }
            }
            eprintln!();
        }
        PasswordSource::EnvVar => {
            eprintln!("[Keyring] Password from environment variable (no keyring involved)");
            eprintln!();
        }
        PasswordSource::None => {
            eprintln!("[Keyring] No password configured");
            eprintln!();
        }
    }

    // ── Phase 2: Connection attempts ──
    let url_without_sslmode = url
        .split_whitespace()
        .filter(|part| !part.starts_with("sslmode="))
        .collect::<Vec<_>>()
        .join(" ");

    struct AttemptResult {
        mode: &'static str,
        success: bool,
        version: Option<String>,
        error: Option<String>,
        verbose_details: Option<VerboseDetails>,
    }

    let mut results: Vec<AttemptResult> = Vec::new();

    eprintln!("[1/3] Trying NoTls (plain TCP) → {} ...", redacted);
    match try_connect_notls(&url_without_sslmode, verbose).await {
        Ok((version, details)) => {
            eprintln!("  ✓ Connected");
            eprintln!("    {}", version);
            if let Some(ref d) = details {
                print_verbose_details(d);
            }
            results.push(AttemptResult { mode: "NoTls", success: true, version: Some(version), error: None, verbose_details: details });
        }
        Err(e) => {
            let chain = format_error_chain(&e);
            eprintln!("  ✗ {}", chain);
            results.push(AttemptResult { mode: "NoTls", success: false, version: None, error: Some(chain), verbose_details: None });
        }
    }

    let tls_url = format!("{} sslmode=require", url_without_sslmode);
    let host_port = parse_host_port_from_url(url);

    eprintln!("[2/3] Trying TLS (skip cert verify) → {} ...", redacted);
    match try_connect_tls(&tls_url, false, verbose).await {
        Ok((version, details)) => {
            eprintln!("  ✓ Connected");
            eprintln!("    {}", version);
            if let Some(ref d) = details {
                print_verbose_details(d);
            }
            if verbose {
                if let Some((ref host, port)) = host_port {
                    match extract_tls_cert_info(host, port, false) {
                        Ok(cert) => print_tls_cert_info(&cert),
                        Err(e) => eprintln!("  [verbose] Certificate extraction skipped: {}", e),
                    }
                }
            }
            results.push(AttemptResult { mode: "TLS (no verify)", success: true, version: Some(version), error: None, verbose_details: details });
        }
        Err(e) => {
            let chain = format_error_chain(e.as_ref());
            eprintln!("  ✗ {}", chain);
            results.push(AttemptResult { mode: "TLS (no verify)", success: false, version: None, error: Some(chain), verbose_details: None });
        }
    }

    eprintln!("[3/3] Trying TLS (verify cert) → {} ...", redacted);
    match try_connect_tls(&tls_url, true, verbose).await {
        Ok((version, details)) => {
            eprintln!("  ✓ Connected");
            eprintln!("    {}", version);
            if let Some(ref d) = details {
                print_verbose_details(d);
            }
            if verbose {
                if let Some((ref host, port)) = host_port {
                    match extract_tls_cert_info(host, port, true) {
                        Ok(cert) => print_tls_cert_info(&cert),
                        Err(e) => eprintln!("  [verbose] Certificate extraction skipped: {}", e),
                    }
                }
            }
            results.push(AttemptResult { mode: "TLS (verify)", success: true, version: Some(version), error: None, verbose_details: details });
        }
        Err(e) => {
            let chain = format_error_chain(e.as_ref());
            eprintln!("  ✗ {}", chain);
            results.push(AttemptResult { mode: "TLS (verify)", success: false, version: None, error: Some(chain), verbose_details: None });
        }
    }

    // ── Summary ──
    eprintln!();
    eprintln!("═══════════════════════════════════════════════════════════");
    eprintln!("  Connection Diagnostic Summary");
    eprintln!("═══════════════════════════════════════════════════════════");

    let mut any_success = false;
    for r in &results {
        if r.success {
            any_success = true;
            let elapsed_str = r.verbose_details.as_ref()
                .map(|d| format!(" ({}ms)", d.elapsed.as_millis()))
                .unwrap_or_default();
            eprintln!("  {:20} ✓  {}{}", r.mode, r.version.as_deref().unwrap_or("(unknown)"), elapsed_str);
        } else {
            eprintln!("  {:20} ✗  {}", r.mode, r.error.as_deref().unwrap_or("unknown"));
        }
    }

    eprintln!();

    if any_success {
        let working = results.iter().find(|r| r.success).unwrap();
        if let Some(ref ver) = working.version {
            eprintln!("  Database Version:");
            eprintln!("    {}", ver);
            eprintln!();
        }
        eprintln!("Recommendation: use {} mode.", working.mode);
        if working.mode != "NoTls" {
            eprintln!("  Add to config: sslmode = \"require\"");
        }
        std::process::exit(0);
    } else {
        eprintln!("All connection methods failed.");
        eprintln!();
        eprintln!("Possible causes:");
        eprintln!("  - Database server is not running or not reachable");
        eprintln!("  - Firewall blocking port 5432");
        eprintln!("  - pg_hba.conf does not allow this client IP/user");
        eprintln!("  - Wrong host, port, user, or password");
        eprintln!("  - Server requires client certificate authentication (cert mode)");
        std::process::exit(1);
    }
}

fn check_keyring_available(username: &str) -> Result<(), String> {
    let test_key = "__gaussdb_mcp_keyring_test__";
    let entry = Entry::new(KEYRING_SERVICE, username)
        .map_err(|e| format!("keyring entry creation failed: {}", e))?;
    entry.set_password(test_key)
        .map_err(|e| format!("keyring write failed: {}", e))?;
    let read_back = entry.get_password()
        .map_err(|e| format!("keyring read-back failed: {}", e))?;
    if read_back != test_key {
        return Err("keyring read-back mismatch".to_string());
    }
    Ok(())
}

async fn query_verbose_details(
    client: &tokio_opengauss::Client,
    elapsed: Duration,
) -> VerboseDetails {
    async fn query_scalar(client: &tokio_opengauss::Client, sql: &str) -> Option<String> {
        match client.query_one(sql, &[]).await {
            Ok(row) => row.try_get::<_, Option<&str>>(0).ok().flatten().map(String::from),
            Err(_) => None,
        }
    }

    let server_version = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'server_version'").await;
    let server_version_num = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'server_version_num'").await;
    let protocol_version = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'protocol_version'").await;
    let current_user = query_scalar(client, "SELECT current_user::text").await;
    let current_database = query_scalar(client, "SELECT current_database()::text").await;
    let server_addr = query_scalar(client, "SELECT inet_server_addr()::text").await;
    let server_port = query_scalar(client, "SELECT inet_server_port()::text").await;
    let start_time = query_scalar(client, "SELECT pg_postmaster_start_time()::text").await;
    let is_in_recovery_str = query_scalar(client, "SELECT pg_is_in_recovery()::text").await;
    let ssl_is_used_str = query_scalar(client, "SELECT ssl_is_used()::text").await;
    let ssl_version = query_scalar(client, "SELECT ssl_version()").await;
    let ssl_cipher = query_scalar(client, "SELECT ssl_cipher()").await;
    let guc_max_connections = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'max_connections'").await;
    let guc_shared_buffers = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'shared_buffers'").await;
    let guc_work_mem = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'work_mem'").await;
    let guc_timezone = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'TimeZone'").await;
    let guc_data_directory = query_scalar(client, "SELECT setting FROM pg_settings WHERE name = 'data_directory'").await;

    let is_in_recovery = is_in_recovery_str
        .as_deref()
        .map(|s| matches!(s.to_lowercase().as_str(), "true" | "t" | "yes" | "on" | "1"));
    let ssl_is_used = ssl_is_used_str
        .as_deref()
        .map(|s| matches!(s.to_lowercase().as_str(), "true" | "t" | "yes" | "on" | "1"));

    VerboseDetails {
        server_version,
        server_version_num,
        protocol_version,
        current_user,
        current_database,
        server_addr,
        server_port,
        start_time,
        is_in_recovery,
        ssl_is_used,
        ssl_version,
        ssl_cipher,
        elapsed,
        guc_max_connections,
        guc_shared_buffers,
        guc_work_mem,
        guc_timezone,
        guc_data_directory,
    }
}

fn extract_tls_cert_info(host: &str, port: u16, verify: bool) -> Result<TlsCertInfo, String> {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    let addr = format!("{}:{}", host, port);
    let mut stream = TcpStream::connect_timeout(
        &addr.parse().map_err(|e| format!("invalid address '{}': {}", addr, e))?,
        Duration::from_secs(5),
    )
    .map_err(|e| format!("TCP connect to {} failed: {}", addr, e))?;

    // Send PostgreSQL SSLRequest message: length=8, protocol=80877103
    let ssl_request: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];
    stream
        .write_all(&ssl_request)
        .map_err(|e| format!("SSL request write failed: {}", e))?;

    let mut buf = [0u8; 1];
    stream
        .read_exact(&mut buf)
        .map_err(|e| format!("SSL response read failed: {}", e))?;

    if buf[0] != b'S' {
        return Err("server does not support TLS".to_string());
    }

    let mut builder = native_tls::TlsConnector::builder();
    if !verify {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let connector = builder
        .build()
        .map_err(|e| format!("TLS connector build failed: {}", e))?;

    let tls_stream = connector
        .connect(host, stream)
        .map_err(|e| format!("TLS handshake failed: {}", e))?;

    let cert = tls_stream
        .peer_certificate()
        .map_err(|e| format!("cert extraction failed: {}", e))?
        .ok_or_else(|| "no peer certificate presented".to_string())?;

    let der = cert
        .to_der()
        .map_err(|e| format!("cert DER encoding failed: {}", e))?;

    let (_, x509) = x509_parser::parse_x509_certificate(&der)
        .map_err(|e| format!("cert parse failed: {:?}", e))?;

    let serial_hex = format!("{:x}", x509.serial);

    Ok(TlsCertInfo {
        subject: x509.subject().to_string(),
        issuer: x509.issuer().to_string(),
        valid_from: x509.validity().not_before.to_rfc2822().map_err(|e| e.to_string())?,
        valid_to: x509.validity().not_after.to_rfc2822().map_err(|e| e.to_string())?,
        serial: serial_hex,
    })
}

fn parse_host_port_from_url(url: &str) -> Option<(String, u16)> {
    let mut host = None;
    let mut port = None;
    for part in url.split_whitespace() {
        if let Some(v) = part.strip_prefix("host=") {
            host = Some(v.trim_matches('"').to_string());
        }
        if let Some(v) = part.strip_prefix("port=") {
            port = v.trim_matches('"').parse::<u16>().ok();
        }
    }
    match (host, port) {
        (Some(h), Some(p)) => Some((h, p)),
        (Some(h), None) => Some((h, 5432)),
        _ => None,
    }
}

fn print_verbose_details(details: &VerboseDetails) {
    eprintln!("  [verbose] Connection Details:");
    eprintln!("    {:24} {}", "server_version", details.server_version.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "server_version_num", details.server_version_num.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "protocol_version", details.protocol_version.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "current_user", details.current_user.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "current_database", details.current_database.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "server_addr", details.server_addr.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "server_port", details.server_port.as_deref().unwrap_or("—"));
    eprintln!("    {:24} {}", "server_start_time", details.start_time.as_deref().unwrap_or("—"));
    match details.is_in_recovery {
        Some(true) => eprintln!("    {:24} true  (standby / recovering)", "is_in_recovery"),
        Some(false) => eprintln!("    {:24} false (primary)", "is_in_recovery"),
        None => eprintln!("    {:24} —", "is_in_recovery"),
    }
    eprintln!("    {:24} {}ms", "connect_time", details.elapsed.as_millis());

    if details.ssl_is_used == Some(true) {
        eprintln!();
        eprintln!("  [verbose] TLS Session:");
        eprintln!("    {:24} {}", "ssl_version", details.ssl_version.as_deref().unwrap_or("—"));
        eprintln!("    {:24} {}", "ssl_cipher", details.ssl_cipher.as_deref().unwrap_or("—"));
    }

    let has_any_guc = details.guc_max_connections.is_some()
        || details.guc_shared_buffers.is_some()
        || details.guc_work_mem.is_some()
        || details.guc_timezone.is_some()
        || details.guc_data_directory.is_some();
    if has_any_guc {
        eprintln!();
        eprintln!("  [verbose] Server Configuration (GUC):");
        eprintln!("    {:24} {}", "max_connections", details.guc_max_connections.as_deref().unwrap_or("—"));
        eprintln!("    {:24} {}", "shared_buffers", details.guc_shared_buffers.as_deref().unwrap_or("—"));
        eprintln!("    {:24} {}", "work_mem", details.guc_work_mem.as_deref().unwrap_or("—"));
        eprintln!("    {:24} {}", "timezone", details.guc_timezone.as_deref().unwrap_or("—"));
        eprintln!("    {:24} {}", "data_directory", details.guc_data_directory.as_deref().unwrap_or("—"));
    }
}

fn print_tls_cert_info(cert: &TlsCertInfo) {
    eprintln!();
    eprintln!("  [verbose] Server Certificate:");
    eprintln!("    {:18} {}", "Subject", cert.subject);
    eprintln!("    {:18} {}", "Issuer", cert.issuer);
    eprintln!("    {:18} {}", "Serial", cert.serial);
    eprintln!("    {:18} {}", "Not Before", cert.valid_from);
    eprintln!("    {:18} {}", "Not After", cert.valid_to);
}

async fn try_connect_notls(url: &str, verbose: bool) -> Result<(String, Option<VerboseDetails>), tokio_opengauss::Error> {
    let start = Instant::now();
    let (client, connection) = tokio_opengauss::connect(url, tokio_opengauss::NoTls).await?;
    let elapsed = start.elapsed();
    tokio::spawn(async move { let _ = connection.await; });

    let row = client.query_one("SELECT version()", &[]).await?;
    let version = row.get::<_, Option<&str>>(0).unwrap_or("(unknown)").to_string();

    let verbose_details = if verbose {
        Some(query_verbose_details(&client, elapsed).await)
    } else {
        None
    };

    Ok((version, verbose_details))
}

async fn try_connect_tls(url: &str, verify: bool, verbose: bool) -> Result<(String, Option<VerboseDetails>), Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let mut builder = native_tls::TlsConnector::builder();
    if !verify {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let connector = builder.build()?;
    let tls = opengauss_native_tls::MakeTlsConnector::new(connector);
    let (client, connection) = tokio_opengauss::connect(url, tls).await?;
    let elapsed = start.elapsed();
    tokio::spawn(async move { let _ = connection.await; });

    let row = client.query_one("SELECT version()", &[]).await?;
    let version = row.get::<_, Option<&str>>(0).unwrap_or("(unknown)").to_string();

    let verbose_details = if verbose {
        Some(query_verbose_details(&client, elapsed).await)
    } else {
        None
    };

    Ok((version, verbose_details))
}

fn print_help() {
    let name = env!("CARGO_PKG_NAME");
    let version = env!("CARGO_PKG_VERSION");
    eprintln!("{name} {version}");
    eprintln!();
    eprintln!("MCP server for openGauss database introspection (stdio transport)");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    gaussdb-mcp [OPTIONS]");
    eprintln!();
    eprintln!("OPTIONS:");
    eprintln!("    -h, --help                Print this help message");
    eprintln!("    --check-connection        Test database connectivity and exit");
    eprintln!("    -v, --verbose             Show detailed connection info (with --check-connection)");
    eprintln!("    --store-password <PASS>   Store password in OS keychain");
    eprintln!("    --config <PATH>           Path to config file (default: ~/.gaussdb-mcp.toml)");
    eprintln!();
    eprintln!("CONFIGURATION (priority order):");
    eprintln!("    1. GAUSSDB_URL / DATABASE_URL environment variable");
    eprintln!("    2. --config <path> CLI argument");
    eprintln!("    3. ~/.gaussdb-mcp.toml default config file");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    # Verify connectivity");
    eprintln!(r#"    GAUSSDB_URL="host=127.0.0.1 user=gaussdb password=Enmo@123 dbname=postgres" gaussdb-mcp --check-connection"#);
    eprintln!();
    eprintln!("    # Verify with detailed info (version, TLS cert, timing)");
    eprintln!(r#"    GAUSSDB_URL="host=127.0.0.1 user=gaussdb password=Enmo@123 dbname=postgres" gaussdb-mcp --check-connection --verbose"#);
    eprintln!();
    eprintln!("    # Store password in OS keychain");
    eprintln!("    gaussdb-mcp --store-password 'MyP@ss123' --config ~/.gaussdb-mcp.toml");
    eprintln!();
    eprintln!("    # Run as MCP server (for Claude/Cursor/etc.)");
    eprintln!(r#"    GAUSSDB_URL="host=127.0.0.1 user=gaussdb password=Enmo@123 dbname=postgres" gaussdb-mcp"#);
}

#[tokio::main]
async fn main() {
    init_logging();

    let args: Vec<String> = std::env::args().collect();

    if args.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return;
    }

    if args.iter().any(|a| a == "--store-password") {
        handle_store_password();
        return;
    }

    if args.iter().any(|a| a == "--check-connection") {
        let verbose = args.iter().any(|a| a == "--verbose" || a == "-v");
        let resolved = resolve_connection_url_or_check();
        handle_check_connection(&resolved, verbose).await;
        return;
    }

    let server = match resolve_connection_url_lazy() {
        LazyResolvedConfig::Ready(resolved) => {
            let server = server::GaussdbMcp::new_disconnected(resolved.connection_url);

            let on_connected = {
                let config_path = resolved.config_path;
                let plaintext_password = Arc::new(resolved.plaintext_password);
                let keyring_username = Arc::new(resolved.keyring_username);
                let migrated = Arc::new(std::sync::atomic::AtomicBool::new(false));
                Arc::new(move || {
                    if migrated.load(std::sync::atomic::Ordering::Relaxed) {
                        return;
                    }
                    if let (Some(path), Some(plaintext)) =
                        (&config_path, plaintext_password.as_ref())
                    {
                        migrated.store(true, std::sync::atomic::Ordering::Relaxed);
                        info!("migrating plaintext password to OS keychain");
                        if let Err(e) = store_keyring_password(&keyring_username, plaintext) {
                            warn!(
                                "failed to store password in keychain: {} (config file NOT modified)",
                                e
                            );
                        } else if let Err(e) = rewrite_password_to_sentinel(path) {
                            warn!("failed to update config file: {}", e);
                        } else {
                            info!("password migrated to OS keychain, config updated");
                        }
                    }
                })
            };
            let server = Arc::new(server.on_connected(on_connected));

            let probe = Arc::clone(&server);
            tokio::spawn(async move {
                probe.try_connect().await;
            });

            server
        }
        LazyResolvedConfig::Pending(resolver) => {
            info!("keychain read deferred, using lazy connection");
            Arc::new(server::GaussdbMcp::new_lazy(resolver))
        }
    };

    info!("starting MCP server on stdio");

    let service = Arc::clone(&server)
        .serve(stdio())
        .await
        .unwrap_or_else(|e| {
            error!("MCP server start failed: {}", e);
            panic!("Failed to start MCP server: {}", e);
        });

    info!("MCP server ready");

    service.waiting().await.unwrap_or_else(|e| {
        error!("MCP server error: {}", e);
        panic!("Server error: {}", e);
    });
}
