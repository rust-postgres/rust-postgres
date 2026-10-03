/// Throughput benchmark for `Client::bind_execute_many`.
///
/// Runs two experiments:
///
/// 1. **Baseline comparison** — `bind_execute_many` (asyncpg defaults) vs per-row `execute`
///    for 1 000 single-column INSERTs.
///
/// 2. **Flush-threshold × row-size matrix** — `bind_execute_many_with_flush_threshold` across
///    nine buffer sizes (512 B → 1 MiB) for four representative row sizes (~35 B, ~200 B,
///    ~1 KiB, ~5 KiB).  Each cell shows rows/s and the ratio vs the asyncpg default (128 KiB)
///    for that same row size, making it easy to see whether the constant holds across workloads.
///
/// Run with:
///   cargo bench -p tokio-postgres --bench bind_execute_many
use std::env;
use std::time::Instant;
use tokio::runtime::Runtime;
use tokio_postgres::{Client, NoTls};

const BASELINE_ROWS: usize = 1_000;
const SWEEP_ROWS: usize = 10_000;
const WARMUP_ITERS: u32 = 2;
const MEASURE_ITERS: u32 = 5;

/// Buffer sizes to sweep. Each entry is `(label, flush_threshold_bytes)`.
const THRESHOLDS: &[(&str, usize)] = &[
    ("512 B", 512),
    ("4 KiB", 4 * 1024),
    ("16 KiB", 16 * 1024),
    ("32 KiB", 32 * 1024),
    ("64 KiB", 64 * 1024),
    ("128 KiB  ← asyncpg default", 128 * 1024),
    ("256 KiB", 256 * 1024),
    ("512 KiB", 512 * 1024),
    ("1 MiB", 1024 * 1024),
];

/// Row sizes to sweep. Each entry is `(label, approx_bytes, payload_len)`.
///
/// `payload_len` is the length of the TEXT string inserted.  The actual wire
/// size per row is larger (Bind header, Execute, format codes, etc.) but the
/// TEXT payload dominates for the wider cases.
const ROW_SIZES: &[(&str, usize, usize)] = &[
    ("~35 B  (INT)", 35, 0), // 0 = use INT column, no text payload
    ("~200 B (TEXT)", 200, 160),
    ("~1 KiB (TEXT)", 1024, 990),
    ("~5 KiB (TEXT)", 5120, 5080),
];

fn conn_string() -> String {
    let host = env::var("POSTGRES_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = env::var("POSTGRES_PORT")
        .unwrap_or_else(|_| "5433".into())
        .parse()
        .unwrap();
    let user = env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".into());
    let password = env::var("POSTGRES_PASSWORD").unwrap_or_else(|_| String::new());
    let dbname = env::var("POSTGRES_DB").unwrap_or_else(|_| "postgres".into());
    format!("host={host} port={port} user={user} password={password} dbname={dbname}")
}

fn setup(rt: &Runtime) -> Client {
    let (client, conn) = rt
        .block_on(tokio_postgres::connect(&conn_string(), NoTls))
        .expect("connect");
    rt.spawn(async move { conn.await.unwrap() });
    client
}

fn measure_bind_execute_many(rt: &Runtime, client: &Client, rows: usize, iters: u32) -> f64 {
    rt.block_on(async {
        client
            .batch_execute("CREATE TEMPORARY TABLE IF NOT EXISTS bench_bem (val INT NOT NULL)")
            .await
            .unwrap();
    });
    let stmt = rt
        .block_on(client.prepare("INSERT INTO bench_bem (val) VALUES ($1)"))
        .unwrap();

    let start = Instant::now();
    for _ in 0..iters {
        rt.block_on(client.bind_execute_many(&stmt, (0i32..rows as i32).map(|i| [i])))
            .unwrap();
        rt.block_on(client.batch_execute("DELETE FROM bench_bem"))
            .unwrap();
    }
    (rows as f64 * iters as f64) / start.elapsed().as_secs_f64()
}

fn measure_per_row_execute(rt: &Runtime, client: &Client, rows: usize, iters: u32) -> f64 {
    rt.block_on(async {
        client
            .batch_execute("CREATE TEMPORARY TABLE IF NOT EXISTS bench_per (val INT NOT NULL)")
            .await
            .unwrap();
    });
    let stmt = rt
        .block_on(client.prepare("INSERT INTO bench_per (val) VALUES ($1)"))
        .unwrap();

    let start = Instant::now();
    for _ in 0..iters {
        for i in 0i32..rows as i32 {
            rt.block_on(client.execute(&stmt, &[&i])).unwrap();
        }
        rt.block_on(client.batch_execute("DELETE FROM bench_per"))
            .unwrap();
    }
    (rows as f64 * iters as f64) / start.elapsed().as_secs_f64()
}

/// Measures `bind_execute_many_with_flush_threshold` for a given row payload.
///
/// `payload_len == 0` → INT column; otherwise TEXT column filled with `payload_len` 'x' chars.
fn measure_with_threshold(
    rt: &Runtime,
    client: &Client,
    rows: usize,
    iters: u32,
    flush_threshold: usize,
    payload_len: usize,
) -> f64 {
    if payload_len == 0 {
        // INT path
        rt.block_on(async {
            client
                .batch_execute(
                    "CREATE TEMPORARY TABLE IF NOT EXISTS bench_sweep_int (val INT NOT NULL)",
                )
                .await
                .unwrap();
        });
        let stmt = rt
            .block_on(client.prepare("INSERT INTO bench_sweep_int (val) VALUES ($1)"))
            .unwrap();

        let start = Instant::now();
        for _ in 0..iters {
            rt.block_on(client.bind_execute_many_with_flush_threshold(
                &stmt,
                (0i32..rows as i32).map(|i| [i]),
                flush_threshold,
            ))
            .unwrap();
            rt.block_on(client.batch_execute("DELETE FROM bench_sweep_int"))
                .unwrap();
        }
        (rows as f64 * iters as f64) / start.elapsed().as_secs_f64()
    } else {
        // TEXT path
        let table = format!("bench_sweep_text_{payload_len}");
        rt.block_on(async {
            client
                .batch_execute(&format!(
                    "CREATE TEMPORARY TABLE IF NOT EXISTS {table} (val TEXT NOT NULL)"
                ))
                .await
                .unwrap();
        });
        let stmt = rt
            .block_on(client.prepare(&format!("INSERT INTO {table} (val) VALUES ($1)")))
            .unwrap();
        let payload: String = "x".repeat(payload_len);

        let start = Instant::now();
        for _ in 0..iters {
            rt.block_on(client.bind_execute_many_with_flush_threshold(
                &stmt,
                (0..rows).map(|_| [payload.as_str()]),
                flush_threshold,
            ))
            .unwrap();
            rt.block_on(client.batch_execute(&format!("DELETE FROM {table}")))
                .unwrap();
        }
        (rows as f64 * iters as f64) / start.elapsed().as_secs_f64()
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    let client = setup(&rt);

    // ── Baseline comparison ───────────────────────────────────────────────────

    println!("\n=== Baseline: bind_execute_many vs per-row execute ({BASELINE_ROWS} rows) ===\n");
    println!("Warming up ({WARMUP_ITERS} iterations each)…");
    measure_bind_execute_many(&rt, &client, BASELINE_ROWS, WARMUP_ITERS);
    measure_per_row_execute(&rt, &client, BASELINE_ROWS, WARMUP_ITERS);

    println!("Measuring ({MEASURE_ITERS} iterations each)…\n");
    let bem_rps = measure_bind_execute_many(&rt, &client, BASELINE_ROWS, MEASURE_ITERS);
    let serial_rps = measure_per_row_execute(&rt, &client, BASELINE_ROWS, MEASURE_ITERS);
    let ratio = bem_rps / serial_rps;

    println!("bind_execute_many : {bem_rps:>10.0} rows/s");
    println!("per-row execute   : {serial_rps:>10.0} rows/s");
    println!();
    println!("bind_execute_many is {ratio:.1}x faster than per-row execute");

    // ── Flush-threshold × row-size matrix ────────────────────────────────────

    println!(
        "\n=== Flush-threshold × row-size matrix ({SWEEP_ROWS} rows, {MEASURE_ITERS} iters each) ===\n"
    );
    println!("Warming up (128 KiB threshold, all row sizes)…");
    for &(_label, _approx, payload_len) in ROW_SIZES {
        measure_with_threshold(
            &rt,
            &client,
            SWEEP_ROWS,
            WARMUP_ITERS,
            128 * 1024,
            payload_len,
        );
    }

    println!("Measuring…\n");

    for &(size_label, approx_bytes, payload_len) in ROW_SIZES {
        println!(
            "── Row size {size_label} (~{approx_bytes} bytes/row × {SWEEP_ROWS} rows ≈ {} KiB wire data) ──",
            approx_bytes * SWEEP_ROWS / 1024
        );
        println!(
            "  {:<32}  {:>7}  {:>10}  {:>8}",
            "flush threshold", "syncs", "rows/s", "vs asyncpg"
        );
        println!("  {}", "-".repeat(62));

        let mut asyncpg_rps = 0f64;
        let mut results: Vec<(&str, usize, u64, f64)> = Vec::new();

        for &(label, threshold) in THRESHOLDS {
            let rps = measure_with_threshold(
                &rt,
                &client,
                SWEEP_ROWS,
                MEASURE_ITERS,
                threshold,
                payload_len,
            );
            let estimated_syncs = (approx_bytes * SWEEP_ROWS).div_ceil(threshold);
            if label.contains("asyncpg") {
                asyncpg_rps = rps;
            }
            results.push((label, threshold, estimated_syncs as u64, rps));
        }

        for (label, _threshold, syncs, rps) in &results {
            let rel = if asyncpg_rps > 0.0 {
                format!("{:.2}x", rps / asyncpg_rps)
            } else {
                "n/a".to_string()
            };
            println!("  {label:<32}  {syncs:>7}  {rps:>10.0}  {rel:>8}");
        }

        let (peak_label, _, _, peak_rps) = results
            .iter()
            .max_by(|a, b| a.3.partial_cmp(&b.3).unwrap())
            .unwrap();
        let gap_pct = (peak_rps - asyncpg_rps) / asyncpg_rps * 100.0;
        println!();
        if asyncpg_rps >= peak_rps * 0.9 {
            println!("  asyncpg default within 10% of peak ({peak_label} @ {peak_rps:.0} rows/s)");
        } else {
            println!(
                "  asyncpg default is {gap_pct:.0}% below peak ({peak_label} @ {peak_rps:.0} rows/s)"
            );
        }
        println!();
    }
}
