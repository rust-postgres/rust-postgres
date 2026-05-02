use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData as McpError},
    tool, tool_handler, tool_router, ServerHandler,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_opengauss::Row;
use tracing::{debug, error, info};

use crate::queries;

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
        "connection_error",
        Some(json!({
            "error": "Database connection failed",
            "detail": chain,
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
    let chain = format_error_chain(err);
    let sql_preview = if sql.len() > 200 { format!("{}...", &sql[..200]) } else { sql.to_string() };
    error!("{} failed: {}", tool, chain);
    McpError::internal_error(
        "database_error",
        Some(json!({
            "error": format!("{} failed", tool),
            "detail": chain,
            "sql": sql_preview,
        })),
    )
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetTableMetadataParams {
    pub table_name: String,
    pub schema_name: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExecuteQueryParams {
    pub sql: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GetExecutionPlanParams {
    pub sql: String,
    pub analyze: Option<bool>,
    pub format: Option<String>,
}

enum ConnectionState {
    Connecting(String),
    Connected(Arc<tokio_opengauss::Client>),
    Unavailable(String),
}

pub struct GaussdbMcp {
    state: Arc<Mutex<ConnectionState>>,
    on_connected: Option<Arc<dyn Fn() + Send + Sync>>,
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
    pub fn new_disconnected(url: String) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState::Connecting(url))),
            on_connected: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new(client: tokio_opengauss::Client) -> Self {
        Self {
            state: Arc::new(Mutex::new(ConnectionState::Connected(Arc::new(client)))),
            on_connected: None,
        }
    }

    pub fn on_connected(mut self, callback: Arc<dyn Fn() + Send + Sync>) -> Self {
        self.on_connected = Some(callback);
        self
    }

    pub async fn try_connect(&self) {
        let url = {
            let state = self.state.lock().await;
            match &*state {
                ConnectionState::Connecting(url) => url.clone(),
                _ => return,
            }
        };

        info!("probing database connection at startup");
        let result = do_connect(&url).await;

        let mut state = self.state.lock().await;
        match result {
            Ok((client, _handle)) => {
                info!("startup probe: database connected successfully");
                if let Some(ref cb) = self.on_connected {
                    cb();
                }
                *state = ConnectionState::Connected(client);
            }
            Err(e) => {
                let chain = format_error_chain(e.as_ref());
                let redacted = redact_url(&url);
                error!("startup probe: database connection failed: {} (target: {})", chain, redacted);
                *state = ConnectionState::Unavailable(url);
            }
        }
    }

    async fn get_client(&self) -> Result<Arc<tokio_opengauss::Client>, McpError> {
        let state = self.state.lock().await;
        match &*state {
            ConnectionState::Connected(client) => Ok(Arc::clone(client)),
            ConnectionState::Unavailable(url) | ConnectionState::Connecting(url) => {
                let url = url.clone();
                drop(state);

                info!("attempting database connection");
                let result = do_connect(&url).await;
                let mut state = self.state.lock().await;

                match result {
                    Ok((client, _handle)) => {
                        info!("database connected successfully");
                        if let Some(ref cb) = self.on_connected {
                            cb();
                        }
                        *state = ConnectionState::Connected(Arc::clone(&client));
                        Ok(client)
                    }
                    Err(e) => {
                        let err = connection_error(&url, e.as_ref());
                        *state = ConnectionState::Unavailable(url);
                        Err(err)
                    }
                }
            }
        }
    }
}

#[tool_router]
impl GaussdbMcp {
    #[tool(description = "Get database version and server information")]
    async fn get_database_info(&self) -> Result<CallToolResult, McpError> {
        info!("tool called: get_database_info");
        let client = self.get_client().await?;
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
    async fn list_tables(&self) -> Result<CallToolResult, McpError> {
        info!("tool called: list_tables");
        let client = self.get_client().await?;
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
        info!("tool called: get_table_metadata schema={} table={}", schema, table);
        let client = self.get_client().await?;

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
        debug!("tool called: execute_query sql_len={}", trimmed.len());
        let client = self.get_client().await?;
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
        info!("tool called: get_execution_plan analyze={} format={}", analyze, format);
        let client = self.get_client().await?;

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
}

#[tool_handler(name = "gaussdb-mcp", version = "0.1.0", instructions = "MCP server for openGauss database introspection")]
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
