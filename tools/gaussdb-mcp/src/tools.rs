use rmcp::{
    handler::server::wrapper::Parameters,
    model::{CallToolResult, Content, ErrorData as McpError},
    tool,
};
use serde_json::json;
use tokio_opengauss::Row;

use crate::queries;

fn sqlstate_to_sqlcode(state: &str) -> i32 {
    match state {
        "00000" => 0, "01000" => 100, "0100C" => 110, "01008" => -12, "01003" => -5,
        "01007" => 0, "01006" => 0, "01004" => 0, "01P01" => 0,
        "02000" => 100, "02001" => 100, "03000" => 0,
        "08000" => -402, "08003" => -26, "08006" => -402, "08001" => -1014, "08004" => -1015,
        "08007" => -402, "08P01" => -402,
        "09000" => 0, "0A000" => -300, "0B000" => -301,
        "0F000" => 0, "0F001" => 0, "0L000" => 0, "0LP01" => 0, "0P000" => 0,
        "0Z000" => 0, "0Z002" => 0,
        "20000" => -200, "21000" => -812,
        "22000" => -302, "2202E" => -302, "22021" => -302, "22008" => -180, "22012" => -802,
        "22005" => -303, "2200B" => -302, "22022" => -302, "22015" => -302,
        "2201E" => -302, "22014" => -302, "22016" => -302, "2201F" => -302, "2201G" => -302,
        "22018" => -302, "22007" => -181, "22019" => -24, "2200D" => -302, "22025" => -24,
        "22P06" => -302, "22010" => -302, "22023" => -302, "22013" => -302, "2201B" => -302,
        "2201W" => -302, "2201X" => -302, "2202H" => -302, "2202G" => -302, "22009" => -302,
        "2200C" => -302, "2200G" => -302, "22004" => -302, "22002" => -305, "22003" => -304,
        "2200H" => -302, "22026" => -302, "22001" => -302, "22011" => -302, "22027" => -302,
        "22024" => -302, "2200F" => -302,
        "22030" | "22031" | "22032" | "22033" | "22034" | "22035" | "22036" | "22037"
        | "22038" | "22039" | "2203A" | "2203B" | "2203C" | "2203D" | "2203E" | "2203F"
        | "2203G" => -302,
        "22P01" => -302, "22P02" => -302, "22P03" => -302, "22P04" => -302, "22P05" => -302,
        "2200L" => -302, "2200M" => -302, "2200N" => -302, "2200S" => -302, "2200T" => -302,
        "23000" => -407, "23001" => -407, "23502" => -407, "23503" => -530, "23505" => -803,
        "23514" => -543, "23P01" => -540,
        "24000" => -501, "25000" => -501, "25001" => -502, "25002" => -503, "25008" => -505,
        "25003" => -508, "25004" => -509, "25005" => -511, "25006" => -513, "25007" => -514,
        "25P01" => -512, "25P02" => -514, "25P03" => -501,
        "26000" => -504, "27000" => -518, "28000" => -923, "28P01" => -923,
        "2B000" => -551, "2BP01" => -551, "2D000" => -502,
        "2F000" => -444, "2F002" => -444, "2F003" => -444, "2F004" => -444, "2F005" => -444,
        "34000" => -506,
        "38000" | "38001" | "38002" | "38003" | "38004" => -390,
        "39000" | "39001" | "39004" | "39P01" | "39P02" | "39P03" => -390,
        "3B000" => -818, "3B001" => -818, "3D000" => -204, "3F000" => -204,
        "40000" => -501, "40001" => -911, "40002" => -911, "40003" => -501, "40P01" => -913,
        "42000" => -199, "42501" => -199, "42601" => -104, "42602" => -10, "42622" => -108,
        "42611" => -110, "42701" => -601, "42702" => -203, "42703" => -206, "42704" => -204,
        "42710" => -601, "42712" => -601, "42723" => -803, "42725" => -562,
        "42803" => -119, "42804" => -120, "42809" => -132, "42830" => -530, "42846" => -132,
        "42883" => -130, "428C9" => -138, "42939" => -324,
        "42P01" => -204, "42P02" => -516, "42P03" => -335, "42P04" => -602, "42P05" => -335,
        "42P06" => -601, "42P07" => -601, "42P08" => -516, "42P09" => -203, "42P10" => -108,
        "42P11" => -130, "42P12" => -108, "42P13" => -130, "42P14" => -108, "42P15" => -108,
        "42P16" => -108, "42P17" => -324, "42P18" => -108, "42P19" => -108, "42P20" => -108,
        "42P21" => -108, "42P22" => -108,
        "44000" => -543, "45000" => -552,
        "53000" => -904, "53100" => -904, "53200" => -1205, "53300" => -901, "53400" => -904,
        "54000" => -901, "54001" => -101, "54011" => -101, "54023" => -101,
        "55000" => -551, "55006" => -551, "55P02" => -551, "55P03" => -551, "55P04" => -551,
        "57000" => -300, "57014" => -952,
        "57P01" | "57P02" | "57P03" | "57P04" | "57P05" => -952,
        "58000" => -402, "58030" => -402, "58P01" => -24, "58P02" => -24,
        "72000" => -202, "F0000" => -444, "F0001" => -444,
        "HV000" | "HV001" | "HV002" | "HV004" | "HV005" | "HV006" | "HV007" | "HV008"
        | "HV009" | "HV00A" | "HV00B" | "HV00C" | "HV00D" | "HV00J" | "HV00K" | "HV00L"
        | "HV00M" | "HV00N" | "HV00P" | "HV00Q" | "HV00R" | "HV010" | "HV014" | "HV021"
        | "HV024" | "HV090" | "HV091" => -390,
        "P0000" | "P0001" | "P0002" | "P0003" | "P0004" => -461,
        "XX000" | "XX001" | "XX002" => -402,
        _ => -1,
    }
}

fn db_query_error(tool: &str, sql: &str, err: &tokio_opengauss::Error) -> McpError {
    if let Some(db_err) = err.as_db_error() {
        let sqlstate = db_err.code().code();
        let sqlcode = sqlstate_to_sqlcode(sqlstate);
        let message = db_err.message();
        let mut data = json!({
            "sqlstate": sqlstate,
            "sqlcode": sqlcode,
            "severity": db_err.severity(),
            "message": message,
            "sql": if sql.len() > 200 { format!("{}...", &sql[..200]) } else { sql.to_string() },
        });
        if let Some(detail) = db_err.detail() {
            data["detail"] = json!(detail);
        }
        if let Some(hint) = db_err.hint() {
            data["hint"] = json!(hint);
        }
        if let Some(pos) = db_err.position() {
            data["position"] = json!(format!("{:?}", pos));
        }
        McpError::internal_error(
            format!("[SQLSTATE {} | SQLCODE {}] {} failed: {}", sqlstate, sqlcode, tool, message),
            Some(data),
        )
    } else {
        McpError::internal_error(format!("{} failed: {}", tool, err), Some(json!({ "sql": sql })))
    }
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
            .map_err(|e| db_query_error("get_database_info", queries::DATABASE_INFO, &e))?;

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
            .map_err(|e| db_query_error("list_tables", queries::LIST_TABLES, &e))?;

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
            .map_err(|e| db_query_error("get_table_metadata (columns)", queries::TABLE_COLUMNS, &e))?;

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
            .map_err(|e| db_query_error("get_table_metadata (primary_keys)", queries::TABLE_PRIMARY_KEYS, &e))?;

        let primary_keys: Vec<String> = pk_rows
            .iter()
            .filter_map(|row| row.get::<_, Option<&str>>(0).map(String::from))
            .collect();

        let idx_rows = self
            .client
            .query(queries::TABLE_INDEXES, &[&schema, &table.as_str()])
            .await
            .map_err(|e| db_query_error("get_table_metadata (indexes)", queries::TABLE_INDEXES, &e))?;

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
            db_query_error("execute_query", trimmed, &e)
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
            db_query_error("get_execution_plan", &explain_sql, &e)
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
