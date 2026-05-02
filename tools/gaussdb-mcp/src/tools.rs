use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData as McpError},
    tool,
};
use serde_json::json;
use tokio_opengauss::Row;

use crate::queries;

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

pub struct Tools {
    pub client: tokio_opengauss::Client,
}

impl Tools {
    pub fn new(client: tokio_opengauss::Client) -> Self {
        Self { client }
    }

    #[tool(description = "Get database version and server information")]
    pub async fn get_database_info(&self) -> Result<CallToolResult, McpError> {
        let row = self
            .client
            .query_one(queries::DATABASE_INFO, &[])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "database_error",
                    Some(json!({ "message": format!("Query failed: {}", e) })),
                )
            })?;

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
    pub async fn list_tables(&self) -> Result<CallToolResult, McpError> {
        let rows = self
            .client
            .query(queries::LIST_TABLES, &[])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "database_error",
                    Some(json!({ "message": format!("Query failed: {}", e) })),
                )
            })?;

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
    pub async fn get_table_metadata(
        &self,
        Parameters(params): Parameters<GetTableMetadataParams>,
    ) -> Result<CallToolResult, McpError> {
        let schema = params.schema_name.as_deref().unwrap_or("public");
        let table = &params.table_name;

        let columns_rows = self
            .client
            .query(queries::TABLE_COLUMNS, &[&schema, &table.as_str()])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "database_error",
                    Some(json!({ "message": format!("Columns query failed: {}", e) })),
                )
            })?;

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

        let pk_rows = self
            .client
            .query(queries::TABLE_PRIMARY_KEYS, &[&schema, &table.as_str()])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "database_error",
                    Some(json!({ "message": format!("Primary key query failed: {}", e) })),
                )
            })?;

        let primary_keys: Vec<String> = pk_rows
            .iter()
            .filter_map(|row| row.get::<_, Option<&str>>(0).map(String::from))
            .collect();

        let idx_rows = self
            .client
            .query(queries::TABLE_INDEXES, &[&schema, &table.as_str()])
            .await
            .map_err(|e| {
                McpError::internal_error(
                    "database_error",
                    Some(json!({ "message": format!("Index query failed: {}", e) })),
                )
            })?;

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
    pub async fn execute_query(
        &self,
        Parameters(params): Parameters<ExecuteQueryParams>,
    ) -> Result<CallToolResult, McpError> {
        let trimmed = params.sql.trim();
        let upper = trimmed.to_uppercase();
        if !upper.starts_with("SELECT") && !upper.starts_with("EXPLAIN") {
            return Err(McpError::invalid_request(
                "invalid_query",
                Some(json!({ "message": "Only SELECT and EXPLAIN queries are allowed" })),
            ));
        }

        let rows = self.client.query(trimmed, &[]).await.map_err(|e| {
            McpError::internal_error(
                "database_error",
                Some(json!({ "message": format!("Query failed: {}", e) })),
            )
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
    pub async fn get_execution_plan(
        &self,
        Parameters(params): Parameters<GetExecutionPlanParams>,
    ) -> Result<CallToolResult, McpError> {
        let analyze = params.analyze.unwrap_or(false);
        let format = params.format.as_deref().unwrap_or("TEXT").to_uppercase();

        let explain_sql = if analyze {
            format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT {}) {}", format, params.sql)
        } else {
            format!("EXPLAIN (FORMAT {}) {}", format, params.sql)
        };

        let rows = self.client.query(&explain_sql, &[]).await.map_err(|e| {
            McpError::internal_error(
                "database_error",
                Some(json!({ "message": format!("EXPLAIN failed: {}", e) })),
            )
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
