use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData as McpError},
    tool, tool_handler, tool_router, ServerHandler,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_opengauss::Row;
use tracing::{debug, error, info};

use crate::queries;

fn sqlstate_to_sqlcode(state: &str) -> i32 {
    match state {
        "00000" => 0,
        "01000" => 100,
        "0100C" => 110,
        "01008" => -12,
        "01003" => -5,
        "01007" => 0,
        "01006" => 0,
        "01004" => 0,
        "01P01" => 0,
        "02000" => 100,
        "02001" => 100,
        "03000" => 0,
        "08000" => -402,
        "08003" => -26,
        "08006" => -402,
        "08001" => -1014,
        "08004" => -1015,
        "08007" => -402,
        "08P01" => -402,
        "09000" => 0,
        "0A000" => -300,
        "0B000" => -301,
        "0F000" => 0,
        "0F001" => 0,
        "0L000" => 0,
        "0LP01" => 0,
        "0P000" => 0,
        "0Z000" => 0,
        "0Z002" => 0,
        "20000" => -200,
        "21000" => -812,
        "22000" => -302,
        "2202E" => -302,
        "22021" => -302,
        "22008" => -180,
        "22012" => -802,
        "22005" => -303,
        "2200B" => -302,
        "22022" => -302,
        "22015" => -302,
        "2201E" => -302,
        "22014" => -302,
        "22016" => -302,
        "2201F" => -302,
        "2201G" => -302,
        "22018" => -302,
        "22007" => -181,
        "22019" => -24,
        "2200D" => -302,
        "22025" => -24,
        "22P06" => -302,
        "22010" => -302,
        "22023" => -302,
        "22013" => -302,
        "2201B" => -302,
        "2201W" => -302,
        "2201X" => -302,
        "2202H" => -302,
        "2202G" => -302,
        "22009" => -302,
        "2200C" => -302,
        "2200G" => -302,
        "22004" => -302,
        "22002" => -305,
        "22003" => -304,
        "2200H" => -302,
        "22026" => -302,
        "22001" => -302,
        "22011" => -302,
        "22027" => -302,
        "22024" => -302,
        "2200F" => -302,
        "22030" | "22031" | "22032" | "22033" | "22034" | "22035" | "22036" | "22037"
        | "22038" | "22039" | "2203A" | "2203B" | "2203C" | "2203D" | "2203E" | "2203F"
        | "2203G" => -302,
        "22P01" => -302,
        "22P02" => -302,
        "22P03" => -302,
        "22P04" => -302,
        "22P05" => -302,
        "2200L" => -302,
        "2200M" => -302,
        "2200N" => -302,
        "2200S" => -302,
        "2200T" => -302,
        "23000" => -407,
        "23001" => -407,
        "23502" => -407,
        "23503" => -530,
        "23505" => -803,
        "23514" => -543,
        "23P01" => -540,
        "24000" => -501,
        "25000" => -501,
        "25001" => -502,
        "25002" => -503,
        "25008" => -505,
        "25003" => -508,
        "25004" => -509,
        "25005" => -511,
        "25006" => -513,
        "25007" => -514,
        "25P01" => -512,
        "25P02" => -514,
        "25P03" => -501,
        "26000" => -504,
        "27000" => -518,
        "28000" => -923,
        "28P01" => -923,
        "2B000" => -551,
        "2BP01" => -551,
        "2D000" => -502,
        "2F000" => -444,
        "2F002" => -444,
        "2F003" => -444,
        "2F004" => -444,
        "2F005" => -444,
        "34000" => -506,
        "38000" => -390,
        "38001" => -390,
        "38002" => -390,
        "38003" => -390,
        "38004" => -390,
        "39000" => -390,
        "39001" => -390,
        "39004" => -390,
        "39P01" => -390,
        "39P02" => -390,
        "39P03" => -390,
        "3B000" => -818,
        "3B001" => -818,
        "3D000" => -204,
        "3F000" => -204,
        "40000" => -501,
        "40001" => -911,
        "40002" => -911,
        "40003" => -501,
        "40P01" => -913,
        "42000" => -199,
        "42501" => -199,
        "42601" => -104,
        "42602" => -10,
        "42622" => -108,
        "42611" => -110,
        "42701" => -601,
        "42702" => -203,
        "42703" => -206,
        "42704" => -204,
        "42710" => -601,
        "42712" => -601,
        "42723" => -803,
        "42725" => -562,
        "42803" => -119,
        "42804" => -120,
        "42809" => -132,
        "42830" => -530,
        "42846" => -132,
        "42883" => -130,
        "428C9" => -138,
        "42939" => -324,
        "42P01" => -204,
        "42P02" => -516,
        "42P03" => -335,
        "42P04" => -602,
        "42P05" => -335,
        "42P06" => -601,
        "42P07" => -601,
        "42P08" => -516,
        "42P09" => -203,
        "42P10" => -108,
        "42P11" => -130,
        "42P12" => -108,
        "42P13" => -130,
        "42P14" => -108,
        "42P15" => -108,
        "42P16" => -108,
        "42P17" => -324,
        "42P18" => -108,
        "42P19" => -108,
        "42P20" => -108,
        "42P21" => -108,
        "42P22" => -108,
        "44000" => -543,
        "45000" => -552,
        "53000" => -904,
        "53100" => -904,
        "53200" => -1205,
        "53300" => -901,
        "53400" => -904,
        "54000" => -901,
        "54001" => -101,
        "54011" => -101,
        "54023" => -101,
        "55000" => -551,
        "55006" => -551,
        "55P02" => -551,
        "55P03" => -551,
        "55P04" => -551,
        "57000" => -300,
        "57014" => -952,
        "57P01" => -952,
        "57P02" => -952,
        "57P03" => -952,
        "57P04" => -952,
        "57P05" => -952,
        "58000" => -402,
        "58030" => -402,
        "58P01" => -24,
        "58P02" => -24,
        "72000" => -202,
        "F0000" => -444,
        "F0001" => -444,
        "HV000" | "HV001" | "HV002" | "HV004" | "HV005" | "HV006" | "HV007" | "HV008"
        | "HV009" | "HV00A" | "HV00B" | "HV00C" | "HV00D" | "HV00J" | "HV00K" | "HV00L"
        | "HV00M" | "HV00N" | "HV00P" | "HV00Q" | "HV00R" | "HV010" | "HV014" | "HV021"
        | "HV024" | "HV090" | "HV091" => -390,
        "P0000" => -461,
        "P0001" => -461,
        "P0002" => -461,
        "P0003" => -461,
        "P0004" => -461,
        "XX000" => -402,
        "XX001" => -402,
        "XX002" => -402,
        _ => -1,
    }
}

pub(crate) fn format_error_chain(err: &dyn std::error::Error) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(e) = source {
        parts.push(e.to_string());
        source = e.source();
    }
    parts.join(" | caused by: ")
}

pub(crate) fn redact_url(url: &str) -> String {
    url.split("password=")
        .enumerate()
        .map(|(i, part)| {
            if i == 0 {
                part.to_string()
            } else if let Some(space_pos) = part.find(' ') {
                format!("****{}", &part[space_pos..])
            } else {
                "****".to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("password=")
}

fn connection_error(url: &str, err: &dyn std::error::Error) -> McpError {
    let chain = format_error_chain(err);
    let redacted = redact_url(url);
    error!("database connection failed: {} (target: {})", chain, redacted);
    McpError::internal_error(
        format!("Database connection failed: {}", chain),
        Some(json!({
            "target": redacted,
            "hints": [
                "Check if the database server is running",
                "Verify host, port, user, and dbname in the connection string",
                "Ensure network connectivity and firewall rules allow the connection",
                "Check if SSL/TLS is required (sslmode=require)",
            ]
        })),
    )
}

fn query_error(tool: &str, sql: &str, err: &tokio_opengauss::Error) -> McpError {
    let sql_preview = if sql.len() > 200 { format!("{}...", &sql[..200]) } else { sql.to_string() };

    if let Some(db_err) = err.as_db_error() {
        let sqlstate = db_err.code().code();
        let sqlcode = sqlstate_to_sqlcode(sqlstate);
        let message = db_err.message();
        error!("[SQLSTATE {}] {} failed: {} - {}", sqlstate, tool, message, db_err.detail().unwrap_or(""));

        let mut data = json!({
            "sqlstate": sqlstate,
            "sqlcode": sqlcode,
            "severity": db_err.severity(),
            "message": message,
            "sql": sql_preview,
        });
        if let Some(detail) = db_err.detail() {
            data["detail"] = json!(detail);
        }
        if let Some(hint) = db_err.hint() {
            data["hint"] = json!(hint);
        }
        if let Some(schema) = db_err.schema() {
            data["schema"] = json!(schema);
        }
        if let Some(table) = db_err.table() {
            data["table"] = json!(table);
        }
        if let Some(column) = db_err.column() {
            data["column"] = json!(column);
        }
        if let Some(constraint) = db_err.constraint() {
            data["constraint"] = json!(constraint);
        }
        if let Some(pos) = db_err.position() {
            data["position"] = json!(format!("{:?}", pos));
        }

        McpError::internal_error(
            format!("[SQLSTATE {} | SQLCODE {}] {} failed: {}", sqlstate, sqlcode, tool, message),
            Some(data),
        )
    } else {
        let chain = format_error_chain(err);
        error!("{} failed: {}", tool, chain);
        McpError::internal_error(
            format!("{} failed: {}", tool, chain),
            Some(json!({
                "sql": sql_preview,
            })),
        )
    }
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ConnectionNameParams {
    #[serde(default)]
    pub connection_name: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTableMetadataParams {
    pub table_name: String,
    pub schema_name: Option<String>,
    #[serde(default)]
    pub connection_name: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExecuteQueryParams {
    pub sql: String,
    #[serde(default)]
    pub connection_name: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetExecutionPlanParams {
    pub sql: String,
    pub analyze: Option<bool>,
    pub format: Option<String>,
    #[serde(default)]
    pub connection_name: Option<String>,
}

enum ConnectionState {
    Pending(Arc<dyn (Fn() -> Result<String, String>) + Send + Sync>),
    Connecting(String),
    Connected(Arc<tokio_opengauss::Client>),
    Unavailable(String),
}

pub struct GaussdbMcp {
    connections: Arc<Mutex<HashMap<String, ConnectionState>>>,
    default_name: String,
    on_connected: HashMap<String, Arc<dyn Fn() + Send + Sync>>,
}

fn needs_tls(url: &str) -> bool {
    url.split_whitespace().any(|part| {
        if let Some(val) = part.strip_prefix("sslmode=") {
            matches!(val, "require" | "verify-ca" | "verify-full")
        } else {
            false
        }
    })
}

async fn do_connect(url: &str) -> Result<(Arc<tokio_opengauss::Client>, tokio::task::JoinHandle<()>), Box<dyn std::error::Error + Send + Sync>> {
    if needs_tls(url) {
        let connector = native_tls::TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()?;
        let tls = opengauss_native_tls::MakeTlsConnector::new(connector);
        let (client, connection) = tokio_opengauss::connect(url, tls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("database connection lost: {}", e);
            }
        });
        Ok((Arc::new(client), handle))
    } else {
        let (client, connection) = tokio_opengauss::connect(url, tokio_opengauss::NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("database connection lost: {}", e);
            }
        });
        Ok((Arc::new(client), handle))
    }
}

impl GaussdbMcp {
    /// Create a multi-connection server with eager (pre-resolved) URLs.
    pub fn new_multi_disconnected(entries: Vec<(String, String)>, default_name: String) -> Self {
        let mut connections = HashMap::new();
        for (name, url) in entries {
            connections.insert(name, ConnectionState::Connecting(url));
        }
        Self {
            connections: Arc::new(Mutex::new(connections)),
            default_name,
            on_connected: HashMap::new(),
        }
    }

    /// Create a multi-connection server with lazy resolvers (deferred keychain reads).
    pub fn new_multi_lazy(
        entries: Vec<(String, Arc<dyn (Fn() -> Result<String, String>) + Send + Sync>)>,
        default_name: String,
    ) -> Self {
        let mut connections = HashMap::new();
        for (name, resolver) in entries {
            connections.insert(name, ConnectionState::Pending(resolver));
        }
        Self {
            connections: Arc::new(Mutex::new(connections)),
            default_name,
            on_connected: HashMap::new(),
        }
    }

    /// Backward-compatible: single connection, eager URL.
    #[allow(dead_code)]
    pub fn new_disconnected(url: String) -> Self {
        Self::new_multi_disconnected(vec![("default".to_string(), url)], "default".to_string())
    }

    /// Backward-compatible: single connection, lazy resolver.
    #[allow(dead_code)]
    pub fn new_lazy(resolver: Arc<dyn (Fn() -> Result<String, String>) + Send + Sync>) -> Self {
        Self::new_multi_lazy(vec![("default".to_string(), resolver)], "default".to_string())
    }

    #[allow(dead_code)]
    pub(crate) fn new(client: tokio_opengauss::Client) -> Self {
        let mut connections = HashMap::new();
        connections.insert("default".to_string(), ConnectionState::Connected(Arc::new(client)));
        Self {
            connections: Arc::new(Mutex::new(connections)),
            default_name: "default".to_string(),
            on_connected: HashMap::new(),
        }
    }

    /// Register a per-connection callback fired on first successful connect.
    pub fn set_on_connected(&mut self, name: String, callback: Arc<dyn Fn() + Send + Sync>) {
        self.on_connected.insert(name, callback);
    }

    /// Backward-compatible builder: sets callback for the default connection.
    #[allow(dead_code)]
    pub fn on_connected(mut self, callback: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.on_connected.insert(self.default_name.clone(), callback);
        self
    }

    /// Probe the default connection at startup.
    pub async fn try_connect(&self) {
        let (name, url) = {
            let conns = self.connections.lock().await;
            match conns.get(&self.default_name) {
                Some(ConnectionState::Connecting(url)) => (self.default_name.clone(), url.clone()),
                _ => return,
            }
        };

        info!("probing database connection '{}' at startup", name);
        let result = do_connect(&url).await;

        let mut conns = self.connections.lock().await;
        match result {
            Ok((client, _handle)) => {
                info!("startup probe: database '{}' connected successfully", name);
                if let Some(cb) = self.on_connected.get(&name) {
                    cb();
                }
                conns.insert(name, ConnectionState::Connected(client));
            }
            Err(e) => {
                let chain = format_error_chain(e.as_ref());
                let redacted = redact_url(&url);
                error!("startup probe: database '{}' connection failed: {} (target: {})", name, chain, redacted);
                conns.insert(name, ConnectionState::Unavailable(url));
            }
        }
    }

    /// Get a client for a named connection (None = default).
    async fn get_client_for(&self, connection_name: Option<&str>) -> Result<Arc<tokio_opengauss::Client>, McpError> {
        let name = connection_name.unwrap_or(&self.default_name).to_string();
        let conns = self.connections.lock().await;

        match conns.get(&name) {
            Some(ConnectionState::Connected(client)) => Ok(Arc::clone(client)),
            Some(ConnectionState::Pending(resolver)) => {
                let resolver = Arc::clone(resolver);
                drop(conns);
                let url = resolver().map_err(|e| {
                    McpError::internal_error(
                        format!("Failed to resolve database credentials for '{}': {}", name, e),
                        Some(json!({
                            "connection_name": name,
                            "hint": "Check your gaussdb-mcp configuration and OS keychain access"
                        })),
                    )
                })?;
                info!("connection URL resolved for '{}', attempting database connection", name);
                self.connect_with_url(name, url).await
            }
            Some(ConnectionState::Connecting(url) | ConnectionState::Unavailable(url)) => {
                let url = url.clone();
                drop(conns);
                info!("attempting database connection for '{}'", name);
                self.connect_with_url(name, url).await
            }
            None => {
                let available: Vec<&String> = conns.keys().collect();
                Err(McpError::invalid_request(
                    "unknown_connection",
                    Some(json!({
                        "message": format!("Connection '{}' not found", name),
                        "available_connections": available,
                        "default_connection": self.default_name,
                    })),
                ))
            }
        }
    }

    /// Backward-compatible: get client for default connection.
    #[allow(dead_code)]
    async fn get_client(&self) -> Result<Arc<tokio_opengauss::Client>, McpError> {
        self.get_client_for(None).await
    }

    async fn connect_with_url(
        &self,
        name: String,
        url: String,
    ) -> Result<Arc<tokio_opengauss::Client>, McpError> {
        let result = do_connect(&url).await;
        let mut conns = self.connections.lock().await;

        match result {
            Ok((client, _handle)) => {
                info!("database '{}' connected successfully", name);
                if let Some(cb) = self.on_connected.get(&name) {
                    cb();
                }
                conns.insert(name, ConnectionState::Connected(Arc::clone(&client)));
                Ok(client)
            }
            Err(e) => {
                let err = connection_error(&url, e.as_ref());
                conns.insert(name, ConnectionState::Unavailable(url));
                Err(err)
            }
        }
    }
}

#[tool_router]
impl GaussdbMcp {
    #[tool(description = "Get database version and server information")]
    async fn get_database_info(
        &self,
        Parameters(params): Parameters<ConnectionNameParams>,
    ) -> Result<CallToolResult, McpError> {
        info!("tool called: get_database_info connection={}", params.connection_name.as_deref().unwrap_or("(default)"));
        let client = self.get_client_for(params.connection_name.as_deref()).await?;
        let row = client
            .query_one(queries::DATABASE_INFO, &[])
            .await
            .map_err(|e| query_error("get_database_info", queries::DATABASE_INFO, &e))?;

        let result = json!({
            "version": row.get::<_, Option<&str>>(0),
            "start_time": row.get::<_, Option<&str>>(1),
            "database": row.get::<_, Option<&str>>(2),
            "current_user": row.get::<_, Option<&str>>(3),
            "server_addr": row.get::<_, Option<&str>>(4),
            "server_port": row.get::<_, Option<&str>>(5),
            "server_version": row.get::<_, Option<&str>>(6),
            "server_encoding": row.get::<_, Option<&str>>(7),
            "lc_collate": row.get::<_, Option<&str>>(8),
        });

        Ok(CallToolResult::success(vec![Content::text(
            result.to_string(),
        )]))
    }

    #[tool(description = "List all user tables and views in the database")]
    async fn list_tables(
        &self,
        Parameters(params): Parameters<ConnectionNameParams>,
    ) -> Result<CallToolResult, McpError> {
        info!("tool called: list_tables connection={}", params.connection_name.as_deref().unwrap_or("(default)"));
        let client = self.get_client_for(params.connection_name.as_deref()).await?;
        let rows = client
            .query(queries::LIST_TABLES, &[])
            .await
            .map_err(|e| query_error("list_tables", queries::LIST_TABLES, &e))?;

        let tables: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                json!({
                    "schema_name": row.get::<_, Option<&str>>(0),
                    "table_name": row.get::<_, Option<&str>>(1),
                    "table_type": row.get::<_, Option<&str>>(2),
                    "total_size": row.get::<_, Option<&str>>(3),
                    "comment": row.get::<_, Option<&str>>(4),
                })
            })
            .collect();

        Ok(CallToolResult::success(vec![Content::text(
            json!(tables).to_string(),
        )]))
    }

    #[tool(description = "Get column metadata, primary keys, and indexes for a specific table")]
    async fn get_table_metadata(
        &self,
        Parameters(params): Parameters<GetTableMetadataParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = params.schema_name.as_deref().unwrap_or("public");
        let table = &params.table_name;
        info!("tool called: get_table_metadata schema={} table={} connection={}", schema, table, params.connection_name.as_deref().unwrap_or("(default)"));
        let client = self.get_client_for(params.connection_name.as_deref()).await?;

        let columns_rows = client
            .query(queries::TABLE_COLUMNS, &[&schema, &table.as_str()])
            .await
            .map_err(|e| query_error("get_table_metadata (columns)", queries::TABLE_COLUMNS, &e))?;

        let columns: Vec<serde_json::Value> = columns_rows
            .iter()
            .map(|row| {
                json!({
                    "column_name": row.get::<_, Option<&str>>(0),
                    "data_type": row.get::<_, Option<&str>>(1),
                    "nullable": row.get::<_, Option<bool>>(2),
                    "default_value": row.get::<_, Option<&str>>(3),
                    "ordinal_position": row.get::<_, Option<i32>>(4),
                    "comment": row.get::<_, Option<&str>>(5),
                })
            })
            .collect();

        let pk_rows = client
            .query(queries::TABLE_PRIMARY_KEYS, &[&schema, &table.as_str()])
            .await
            .map_err(|e| query_error("get_table_metadata (primary_keys)", queries::TABLE_PRIMARY_KEYS, &e))?;

        let primary_keys: Vec<String> = pk_rows
            .iter()
            .filter_map(|row| row.get::<_, Option<&str>>(0).map(String::from))
            .collect();

        let idx_rows = client
            .query(queries::TABLE_INDEXES, &[&schema, &table.as_str()])
            .await
            .map_err(|e| query_error("get_table_metadata (indexes)", queries::TABLE_INDEXES, &e))?;

        let indexes: Vec<serde_json::Value> = idx_rows
            .iter()
            .map(|row| {
                json!({
                    "index_name": row.get::<_, Option<&str>>(0),
                    "is_unique": row.get::<_, Option<bool>>(1),
                    "is_primary": row.get::<_, Option<bool>>(2),
                    "index_def": row.get::<_, Option<&str>>(3),
                })
            })
            .collect();

        let result = json!({
            "columns": columns,
            "primary_keys": primary_keys,
            "indexes": indexes,
        });

        Ok(CallToolResult::success(vec![Content::text(
            result.to_string(),
        )]))
    }

    #[tool(description = "Execute a read-only SQL query (SELECT or EXPLAIN only)")]
    async fn execute_query(
        &self,
        Parameters(params): Parameters<ExecuteQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let trimmed = params.sql.trim();
        let upper = trimmed.to_uppercase();
        debug!("tool called: execute_query sql_len={} connection={}", trimmed.len(), params.connection_name.as_deref().unwrap_or("(default)"));
        let client = self.get_client_for(params.connection_name.as_deref()).await?;
        if !upper.starts_with("SELECT") && !upper.starts_with("EXPLAIN") {
            error!("execute_query rejected non-SELECT query: {:?}", &trimmed[..trimmed.len().min(80)]);
            return Err(McpError::invalid_request(
                "invalid_query",
                Some(json!({ "message": "Only SELECT and EXPLAIN queries are allowed" })),
            ));
        }

        let rows = client.query(trimmed, &[]).await.map_err(|e| {
            query_error("execute_query", trimmed, &e)
        })?;

        if rows.is_empty() {
            return Ok(CallToolResult::success(vec![Content::text(
                json!({"columns": [], "rows": [], "row_count": 0}).to_string(),
            )]));
        }

        let columns: Vec<String> = rows[0]
            .columns()
            .iter()
            .map(|c| c.name().to_string())
            .collect();

        let mut result_rows: Vec<Vec<serde_json::Value>> = Vec::new();
        for row in &rows {
            let mut result_row: Vec<serde_json::Value> = Vec::new();
            for idx in 0..row.len() {
                result_row.push(format_row_value(row, idx));
            }
            result_rows.push(result_row);
        }

        let result = json!({
            "columns": columns,
            "rows": result_rows,
            "row_count": rows.len(),
        });

        Ok(CallToolResult::success(vec![Content::text(
            result.to_string(),
        )]))
    }

    #[tool(description = "Get the execution plan for a SQL query")]
    async fn get_execution_plan(
        &self,
        Parameters(params): Parameters<GetExecutionPlanParams>,
    ) -> Result<CallToolResult, McpError> {
        let analyze = params.analyze.unwrap_or(false);
        let format = params.format.as_deref().unwrap_or("TEXT").to_uppercase();
        info!("tool called: get_execution_plan analyze={} format={} connection={}", analyze, format, params.connection_name.as_deref().unwrap_or("(default)"));
        let client = self.get_client_for(params.connection_name.as_deref()).await?;

        let explain_sql = if analyze {
            format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT {}) {}", format, params.sql)
        } else {
            format!("EXPLAIN (FORMAT {}) {}", format, params.sql)
        };

        let rows = client.query(&explain_sql, &[]).await.map_err(|e| {
            query_error("get_execution_plan", &explain_sql, &e)
        })?;

        let plan = if format == "JSON" {
            if let Some(row) = rows.first() {
                if let Ok(v) = row.try_get::<_, Option<&str>>(0) {
                    v.map(String::from).unwrap_or_default()
                } else {
                    String::new()
                }
            } else {
                String::new()
            }
        } else {
            rows.iter()
                .filter_map(|row| row.try_get::<_, Option<&str>>(0).ok().flatten())
                .collect::<Vec<&str>>()
                .join("\n")
        };

        let result = json!({ "plan": plan });

        Ok(CallToolResult::success(vec![Content::text(
            result.to_string(),
        )]))
    }

    #[tool(description = "List all configured database connections")]
    async fn list_connections(&self) -> Result<CallToolResult, McpError> {
        info!("tool called: list_connections");
        let conns = self.connections.lock().await;
        let connections: Vec<serde_json::Value> = conns
            .iter()
            .map(|(name, state)| {
                let status = match state {
                    ConnectionState::Connected(_) => "connected",
                    ConnectionState::Connecting(_) => "connecting",
                    ConnectionState::Pending(_) => "pending",
                    ConnectionState::Unavailable(_) => "unavailable",
                };
                json!({
                    "name": name,
                    "status": status,
                    "is_default": name == &self.default_name,
                })
            })
            .collect();

        let result = json!({
            "connections": connections,
            "default_connection": self.default_name,
        });

        Ok(CallToolResult::success(vec![Content::text(
            result.to_string(),
        )]))
    }
}

#[tool_handler(name = "gaussdb-mcp", version = "0.2.0", instructions = "MCP server for openGauss database introspection with multi-connection support")]
impl ServerHandler for GaussdbMcp {}

fn format_row_value(row: &Row, idx: usize) -> serde_json::Value {
    if let Ok(v) = row.try_get::<_, Option<&str>>(idx) {
        return serde_json::Value::from(v.map(String::from));
    }
    if let Ok(v) = row.try_get::<_, Option<i32>>(idx) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<_, Option<i64>>(idx) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<_, Option<f64>>(idx) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<_, Option<bool>>(idx) {
        return serde_json::json!(v);
    }
    if let Ok(v) = row.try_get::<_, Option<&[u8]>>(idx) {
        return serde_json::json!(v.map(|b| format!("\\x{}", hex_bytes(b))));
    }
    serde_json::Value::Null
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        result.push_str(&format!("{:02x}", b));
    }
    result
}
