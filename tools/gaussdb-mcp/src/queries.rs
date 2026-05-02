pub const DATABASE_INFO: &str = "
SELECT
    version()::text AS version,
    pg_postmaster_start_time()::text AS start_time,
    current_database()::text AS database,
    current_user::text AS current_user,
    inet_server_addr()::text AS server_addr,
    inet_server_port()::text AS server_port,
    (SELECT setting FROM pg_settings WHERE name = 'server_version')::text AS server_version,
    (SELECT setting FROM pg_settings WHERE name = 'server_encoding')::text AS server_encoding,
    (SELECT setting FROM pg_settings WHERE name = 'lc_collate')::text AS lc_collate
";

pub const LIST_TABLES: &str = "
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    CASE c.relkind
        WHEN 'r' THEN 'table'
        WHEN 'v' THEN 'view'
        WHEN 'm' THEN 'materialized_view'
        WHEN 'f' THEN 'foreign_table'
        WHEN 'p' THEN 'partitioned_table'
    END AS table_type,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
    obj_description(c.oid, 'pg_class') AS comment
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'db4ai', 'dbe_pldebugger', 'dbe_pldeveloper', 'pkg_service', 'sqladvisor', 'blockchain', 'cstore', 'snapshot')
ORDER BY n.nspname, c.relname
";

pub const TABLE_COLUMNS: &str = "
SELECT
    a.attname::text AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod)::text AS data_type,
    NOT a.attnotnull AS nullable,
    pg_catalog.pg_get_expr(d.adbin, d.adrelid)::text AS default_value,
    a.attnum::int4 AS ordinal_position,
    col_description(a.attrelid, a.attnum)::text AS comment
FROM pg_catalog.pg_attribute a
LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum)
WHERE a.attrelid = (
    SELECT c.oid FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relname = $2 AND n.nspname = $1
)
AND NOT a.attisdropped
AND attnum > 0
ORDER BY a.attnum
";

pub const TABLE_PRIMARY_KEYS: &str = "
SELECT
    kcu.column_name
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = $1
  AND tc.table_name = $2
ORDER BY kcu.ordinal_position
";

pub const TABLE_INDEXES: &str = "
SELECT
    i.relname::text AS index_name,
    ix.indisunique AS is_unique,
    ix.indisprimary AS is_primary,
    pg_catalog.pg_get_indexdef(ix.indexrelid)::text AS index_def
FROM pg_catalog.pg_index ix
JOIN pg_catalog.pg_class t ON t.oid = ix.indrelid
JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
JOIN pg_catalog.pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname = $1 AND t.relname = $2
ORDER BY i.relname
";
