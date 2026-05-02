# Rust-openGauss

A native, pure-Rust client library for [openGauss](https://opengauss.org/), providing both synchronous and asynchronous APIs.

Built on the openGauss/PostgreSQL wire protocol (v3.0+) with **zero FFI dependencies** — no libpq, no C libraries.

## Architecture

```
┌──────────────────────────────────────────────────┐
│  gaussdb-mcp (MCP server)                        │
│  Standalone binary for AI assistants             │
│  stdio transport, keychain password mgmt         │
└──────────────────┬───────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────┐
│  opengauss (sync)                                │
│  Thin wrapper over tokio-opengauss               │
│  Blocks on async futures via tokio::Runtime      │
└──────────────────┬───────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────┐
│  tokio-opengauss (async)                         │
│  Native async/await client with pipelining       │
│  Built on tokio AsyncRead + AsyncWrite           │
└──────────────────┬───────────────────────────────┘
                   │
┌──────────────────▼───────────────────────────────┐
│  opengauss-protocol + opengauss-types            │
│  Wire protocol codec and type conversions        │
│  Pure data — no I/O, runtime-agnostic            │
└──────────────────────────────────────────────────┘
```

## Crates

### gaussdb-mcp

A standalone MCP (Model Context Protocol) server for openGauss database introspection. Designed for use with AI assistants like Claude, Cursor, and other MCP-compatible tools.

**Features:**

- 5 MCP tools: `get_database_info`, `list_tables`, `get_table_metadata`, `execute_query`, `get_execution_plan`
- Secure password management via OS keychain (macOS Keychain / Windows Credential Manager / Linux Secret Service)
- TLS support with automatic mode detection (NoTls / skip-verify / full-verify)
- Connection diagnostics via `--check-connection`
- File-based logging (no interference with stdio MCP transport)
- Automatic plaintext → keychain password migration on first successful connection

**Quick Start:**

```sh
# Build
cargo build -p gaussdb-mcp

# Option 1: Connect via environment variable
export GAUSSDB_URL="host=127.0.0.1 user=gaussdb password=secret dbname=postgres"
gaussdb-mcp

# Option 2: Use a config file (~/.gaussdb-mcp.toml)
cat > ~/.gaussdb-mcp.toml << 'EOF'
host = "127.0.0.1"
port = 5432
user = "gaussdb"
password = "secret"
dbname = "postgres"
EOF
gaussdb-mcp
```

**CLI Options:**

```
gaussdb-mcp [OPTIONS]

OPTIONS:
    -h, --help                Print help message
    --check-connection        Test database connectivity and exit
    --store-password <PASS>   Store password in OS keychain
    --config <PATH>           Path to config file (default: ~/.gaussdb-mcp.toml)
```

**Connection Diagnostics:**

```sh
gaussdb-mcp --check-connection
# Output:
# [Keyring] Password from config file (plaintext)
#   ✓ OS keychain is available — password will be migrated on first MCP connection
#
# [1/3] Trying NoTls (plain TCP) → host=127.0.0.1 ... user=gaussdb password=**** ...
#   ✓ Success
# [2/3] Trying TLS (skip cert verify) → ...
#   ✗ server does not support TLS
# [3/3] Trying TLS (verify cert) → ...
#   ✗ server does not support TLS
#
# Recommendation: use NoTls mode.
```

**Password Management:**

```sh
# Store password in OS keychain (replaces plaintext in config)
gaussdb-mcp --store-password 'MyP@ss123' --config ~/.gaussdb-mcp.toml

# After first successful MCP connection with plaintext password,
# it is automatically migrated to the OS keychain.
# Config file is updated: password = "keyring"
```

**MCP Tool Reference:**

| Tool | Description |
|------|-------------|
| `get_database_info` | Server version, encoding, collation, start time, current user |
| `list_tables` | All tables in specified schema (default: current user schema) |
| `get_table_metadata` | Columns, types, defaults, nullable, indexes, constraints |
| `execute_query` | Execute SELECT queries with parameterized SQL (`$1`, `$2`, ...) |
| `get_execution_plan` | `EXPLAIN` or `EXPLAIN ANALYZE` output for a query |

**Integration with AI Assistants:**

For Claude Desktop, add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "gaussdb": {
      "command": "/path/to/gaussdb-mcp",
      "env": {
        "GAUSSDB_URL": "host=127.0.0.1 user=gaussdb password=secret dbname=postgres"
      }
    }
  }
}
```

For Cursor, add to `.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "gaussdb": {
      "command": "/path/to/gaussdb-mcp",
      "env": {
        "GAUSSDB_URL": "host=127.0.0.1 user=gaussdb password=secret dbname=postgres"
      }
    }
  }
}
```

### opengauss [![Latest Version](https://img.shields.io/crates/v/opengauss.svg)](https://crates.io/crates/opengauss)

[Documentation](https://docs.rs/opengauss)

A native, synchronous openGauss client. Internally wraps `tokio-opengauss` with a `tokio::Runtime` and blocks on futures.

```rust
use opengauss::{Client, NoTls};

let mut client = Client::connect("host=localhost user=postgres", NoTls)?;

client.batch_execute("
    CREATE TABLE person (
        id      SERIAL PRIMARY KEY,
        name    TEXT NOT NULL,
        data    BYTEA
    )
")?;

let name = "Ferris";
let data = None::<&[u8]>;
client.execute(
    "INSERT INTO person (name, data) VALUES ($1, $2)",
    &[&name, &data],
)?;

for row in client.query("SELECT id, name, data FROM person", &[])? {
    let id: i32 = row.get(0);
    let name: &str = row.get(1);
    let data: Option<&[u8]> = row.get(2);
    println!("found person: {} {} {:?}", id, name, data);
}
```

### tokio-opengauss [![Latest Version](https://img.shields.io/crates/v/tokio-opengauss.svg)](https://crates.io/crates/tokio-opengauss)

[Documentation](https://docs.rs/tokio-opengauss)

A native, asynchronous openGauss client. Supports automatic **pipelining** for concurrent queries.

```rust
use tokio_opengauss::{NoTls, Error};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let (client, connection) =
        tokio_opengauss::connect("host=localhost user=postgres", NoTls).await?;

    // The connection object drives communication with the database.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let rows = client
        .query("SELECT $1::TEXT", &[&"hello world"])
        .await?;

    let value: &str = rows[0].get(0);
    assert_eq!(value, "hello world");

    Ok(())
}
```

#### Pipelining

Requests are sent to the server when futures are first polled. When multiple futures are polled concurrently (e.g. via `join`), requests are automatically pipelined — sent upfront without waiting for prior responses:

```rust
use futures_util::future;
use tokio_opengauss::{Client, Error, Statement};

async fn pipelined_prepare(
    client: &Client,
) -> Result<(Statement, Statement), Error> {
    future::try_join(
        client.prepare("SELECT * FROM foo"),
        client.prepare("INSERT INTO bar (id, name) VALUES ($1, $2)"),
    ).await
}
```

### opengauss-types [![Latest Version](https://img.shields.io/crates/v/opengauss-types.svg)](https://crates.io/crates/opengauss-types)

[Documentation](https://docs.rs/opengauss-types)

Conversions between Rust and openGauss types. Implements the `ToSql` and `FromSql` traits with optional integrations for popular crates.

### opengauss-native-tls [![Latest Version](https://img.shields.io/crates/v/opengauss-native-tls.svg)](https://crates.io/crates/opengauss-native-tls)

[Documentation](https://docs.rs/opengauss-native-tls)

TLS support via [native-tls](https://crates.io/crates/native-tls).

### opengauss-openssl [![Latest Version](https://img.shields.io/crates/v/opengauss-openssl.svg)](https://crates.io/crates/opengauss-openssl)

[Documentation](https://docs.rs/opengauss-openssl)

TLS support via [openssl](https://crates.io/crates/openssl).

## Features

### Authentication

Supports openGauss-specific authentication methods in addition to standard PostgreSQL auth:

| Method | Description |
|--------|-------------|
| SHA256 Password | openGauss RFC 5802-based SHA256 authentication |
| MD5 + SHA256 | Combined MD5/SHA256 authentication |
| SM3 Password | Chinese national standard (SM3) |
| SCRAM-SHA-256 | Standard SCRAM authentication |
| MD5 Password | Legacy MD5 authentication |
| Cleartext Password | Plaintext (use with TLS) |

### TLS Support

Both `opengauss-native-tls` and `opengauss-openssl` provide TLS encryption. The `gaussdb-mcp` binary uses `opengauss-native-tls` and supports `sslmode=` parameter in connection URLs:

```sh
# Disable TLS (default)
GAUSSDB_URL="host=127.0.0.1 user=gaussdb dbname=postgres sslmode=disable"

# Require TLS, skip certificate verification
GAUSSDB_URL="host=127.0.0.1 user=gaussdb dbname=postgres sslmode=require"

# Require TLS with full certificate verification
GAUSSDB_URL="host=db.example.com user=gaussdb dbname=postgres sslmode=verify-full"
```

### Runtime Flexibility

The `runtime` Cargo feature (enabled by default) provides convenience APIs using `tokio`. When disabled, all tokio runtime dependencies are removed — the client works with any stream implementing `AsyncRead + AsyncWrite`.

### Type Integrations

Optional `ToSql`/`FromSql` implementations for popular Rust crates:

| Feature | Crate |
|---------|-------|
| `with-chrono-0_4` | [chrono](https://crates.io/crates/chrono) 0.4 |
| `with-jiff-0_1` | [jiff](https://crates.io/crates/jiff) 0.1 |
| `with-jiff-0_2` | [jiff](https://crates.io/crates/jiff) 0.2 |
| `with-serde_json-1` | [serde_json](https://crates.io/crates/serde_json) 1.x |
| `with-uuid-1` | [uuid](https://crates.io/crates/uuid) 1.x |
| `with-time-0_3` | [time](https://crates.io/crates/time) 0.3 |
| `with-bit-vec-0_6` through `0_9` | [bit-vec](https://crates.io/crates/bit-vec) |
| `with-geo-types-0_7` | [geo-types](https://crates.io/crates/geo-types) 0.7 |
| `with-eui48-1` | [eui48](https://crates.io/crates/eui48) 1.x |
| `with-cidr-0_3` | [cidr](https://crates.io/crates/cidr) 0.3 |
| `with-smol_str-01` | [smol_str](https://crates.io/crates/smol_str) 0.1 |

## Running the test suite

The test suite requires a PostgreSQL-compatible server. The easiest way is with Docker:

```sh
# Start the server
docker-compose up -d

# Run tests
cargo test

# Stop the server
docker-compose stop
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
