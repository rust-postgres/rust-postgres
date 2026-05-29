# gaussdb-mcp

A standalone MCP (Model Context Protocol) server for [openGauss](https://opengauss.org/) database introspection. Designed for use with AI assistants like Claude, Cursor, and other MCP-compatible tools.

Built on the openGauss/PostgreSQL wire protocol (v3.0+) with **zero FFI dependencies** — no libpq, no C libraries.

## Features

- **Multi-connection support** — configure multiple named databases in a single TOML file, switch between them per tool call
- 6 MCP tools: `list_connections`, `get_database_info`, `list_tables`, `get_table_metadata`, `execute_query`, `get_execution_plan`
- Secure password management via OS keychain (macOS Keychain / Windows Credential Manager / Linux Secret Service)
- TLS support with automatic mode detection (NoTls / skip-verify / full-verify)
- Connection diagnostics via `--check-connection`
- File-based logging (no interference with stdio MCP transport)
- Automatic plaintext → keychain password migration on first successful connection

## Quick Start

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

## Configuration

### Single Connection (backward compatible)

```toml
host = "127.0.0.1"
port = 5432
user = "gaussdb"
password = "secret"
dbname = "postgres"
```

### Multiple Named Connections

```toml
default_connection = "dev"

[[connections]]
name = "dev"
host = "127.0.0.1"
port = 5432
user = "gaussdb"
password = "secret"
dbname = "devdb"

[[connections]]
name = "prod"
host = "192.168.1.10"
port = 5432
user = "admin"
password = "keyring"     # stored in OS keychain
dbname = "production"

[[connections]]
name = "staging"
url = "host=10.0.0.5 user=admin password=keyring dbname=staging sslmode=require"
```

When `[[connections]]` is present, the flat fields (`host`, `user`, etc.) at the top level are ignored. When absent, the flat fields are wrapped into a single `"default"` connection — fully backward compatible.

`default_connection` specifies which connection is used when tools don't provide a `connection_name`. Defaults to the first connection.

Each connection's password can be:
- Plaintext string — migrated to OS keychain on first successful connection
- `"keyring"` — read from OS keychain (use `--store-password` to set)

## CLI Options

```
gaussdb-mcp [OPTIONS]

OPTIONS:
    -h, --help                Print help message
    --check-connection [NAME] Test database connectivity and exit
    -v, --verbose             Show detailed connection info (with --check-connection)
    --store-password <PASS>   Store password in OS keychain
    --name <NAME>             Target connection name (for --store-password)
    --config <PATH>           Path to config file (default: ~/.gaussdb-mcp.toml)
```

### Connection Diagnostics

```sh
# Check the default connection
gaussdb-mcp --check-connection

# Check a specific named connection
gaussdb-mcp --check-connection prod --config ~/.gaussdb-mcp.toml

# Verbose output (version, TLS cert, timing, server config)
gaussdb-mcp --check-connection --verbose
```

### Password Management

```sh
# Store password for the first/default connection
gaussdb-mcp --store-password 'MyP@ss123' --config ~/.gaussdb-mcp.toml

# Store password for a named connection
gaussdb-mcp --store-password 'Pr0dP@ss' --name prod --config ~/.gaussdb-mcp.toml

# After first successful MCP connection with plaintext password,
# it is automatically migrated to the OS keychain.
# Config file is updated: password = "keyring"
```

## MCP Tool Reference

| Tool | Description |
|------|-------------|
| `list_connections` | List all configured connections with status |
| `get_database_info` | Server version, encoding, collation, start time, current user |
| `list_tables` | All tables and views in the database |
| `get_table_metadata` | Columns, types, defaults, nullable, indexes, constraints |
| `execute_query` | Execute SELECT or EXPLAIN queries |
| `get_execution_plan` | `EXPLAIN` or `EXPLAIN ANALYZE` output for a query |

All tools accept an optional `connection_name` parameter to target a specific database. When omitted, the `default_connection` is used.

### Tool Parameters

**`list_connections`** — no parameters.

**`get_database_info`**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connection_name` | string | no | Target connection name |

**`list_tables`**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connection_name` | string | no | Target connection name |

**`get_table_metadata`**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `table_name` | string | yes | Table name |
| `schema_name` | string | no | Schema name (default: public) |
| `connection_name` | string | no | Target connection name |

**`execute_query`**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | yes | SQL query (SELECT or EXPLAIN only) |
| `connection_name` | string | no | Target connection name |

**`get_execution_plan`**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `sql` | string | yes | SQL query to explain |
| `analyze` | boolean | no | Run EXPLAIN ANALYZE (default: false) |
| `format` | string | no | Output format: TEXT, JSON, YAML, XML (default: TEXT) |
| `connection_name` | string | no | Target connection name |

## Integration with AI Assistants

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

For multi-connection setups, use a config file instead of the environment variable:

```json
{
  "mcpServers": {
    "gaussdb": {
      "command": "/path/to/gaussdb-mcp",
      "args": ["--config", "/path/to/gaussdb-mcp.toml"]
    }
  }
}
```

## TLS Support

Supports `sslmode=` parameter in connection URLs:

```sh
# Disable TLS (default)
GAUSSDB_URL="host=127.0.0.1 user=gaussdb dbname=postgres sslmode=disable"

# Require TLS, skip certificate verification
GAUSSDB_URL="host=127.0.0.1 user=gaussdb dbname=postgres sslmode=require"

# Require TLS with full certificate verification
GAUSSDB_URL="host=db.example.com user=gaussdb dbname=postgres sslmode=verify-full"
```

## Authentication

Supports openGauss-specific authentication methods in addition to standard PostgreSQL auth:

| Method | Description |
|--------|-------------|
| SHA256 Password | openGauss RFC 5802-based SHA256 authentication |
| MD5 + SHA256 | Combined MD5/SHA256 authentication |
| SM3 Password | Chinese national standard (SM3) |
| SCRAM-SHA-256 | Standard SCRAM authentication |
| MD5 Password | Legacy MD5 authentication |
| Cleartext Password | Plaintext (use with TLS) |

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
