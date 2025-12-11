/*!
 * awr_io_analyze.rs — Oracle AWR I/O Analyzer (Rust)
 *
 * Quickly extract and analyze Oracle AWR (Automatic Workload Repository) I/O-related tables
 * directly from a standard AWR text report, outputting the original table along with
 * actionable expert comments and problem alerts underneath.
 *
 * Usage:
 *   awr_io_analyze <awrrpt_xxx.txt> [config.toml]
 *
 * - Reads any Oracle AWR text report (plain or HTML-to-txt)
 * - Extracts and prints the three key I/O tables, as formatted in the report:
 *      1. Top 10 Foreground Events by Total Wait Time
 *      2. Wait Classes by Total Wait Time
 *      3. IO Profile
 * - Under each table: prints contextual alerts if thresholds are exceeded
 * - Ends with a mini Knowledge Base / Best Practices for quick reference
 *
 * Co-developed by Laurence Oberman and ChatGPT (OpenAI), 2025.
 * License: GPLv3+
 */

// Increment as tool evolves
const VERSION_NUMBER: &str = "1.1.0";

/*
Major Foreground & Background Wait Events

For quick reference (and copy-paste into event string matches):

Foreground:
db file sequential read
db file scattered read
direct path read
direct path write
log file sync
log file parallel write
buffer busy waits
enq: (various: TX, TM, CF, HW, PS, etc)
gc cr multi block mixed, gc cr block busy, etc (GC/RAC)

Background:
log file parallel write
db file parallel write
db file async I/O submit
RMAN backup & recovery I/O
checkpoint completed
*/

/* ============================================================================
   HOW TO ADD A NEW ALERT TYPE (REFERENCE EXAMPLE)
   Example: Alert if DB CPU % > threshold (default 80%)
   ============================================================================

   Step 1 — Add a new field to AlertThresholds in thresholds.rs:

       pub db_cpu_pct: f64,

   Step 2 — Add default & config value in `impl Default`:
       db_cpu_pct: 80.0,

   Also add this to your awr_io_analyze.toml:
       db_cpu_pct = 80.0

   --------------------------------------------------------------------------

   Step 3 — Add logic to your alert function.
   For DB CPU, the row appears inside the Foreground Wait Events table,
   usually as:
        "DB CPU           <value>     <value>      <value>    59.0   "

   Add this inside alert_on_fg_waits(...):

       // DB CPU % threshold check
       if row.contains("DB CPU") {
           if let Some(pct) = extract_percent_from_wait_row(row) {
               if pct > t.db_cpu_pct {
                   alerts.push(format!(
                       "🔵 INFO: DB CPU {:.1}% exceeds threshold {}% — CPU-bound workload.",
                        pct, t.db_cpu_pct
                   ));
               }
           }
       }

   --------------------------------------------------------------------------

   Step 4 — Pass the threshold object into alert_on_fg_waits()

   Change function signature from:
       fn alert_on_fg_waits(table: &[String]) -> Vec<String>

   To:
       fn alert_on_fg_waits(table: &[String], t: &AlertThresholds) -> Vec<String>

   And update your call site in print_table_with_alert():
       let alerts = alert_fn(&table, thresholds);

   --------------------------------------------------------------------------

   Step 5 — Recompile and test with an AWR having high DB CPU.

   Done! You have added a fully configurable, TOML-driven alert.

   ============================================================================
*/

mod thresholds;
use thresholds::{AlertThresholds, load_thresholds_from_file};

use regex::Regex;
use std::env;
use std::fs;
use std::process;


/* ============================================================================
   HOW TO ADD A NEW ALERT TYPE (REFERENCE EXAMPLE)
   (unchanged, keeping your excellent embedded documentation)
   ============================================================================
*/


/// Usage output
fn usage() {
    eprintln!("
Oracle AWR I/O Analyzer (Rust)
------------------------------

Usage:
  awr_io_analyze <awrrpt_xxx.txt> [config.toml]

  Note!! This only works with per-node AWR reports not global reports
  Make sure you ask for per-node AWR reports
   
  For the config.toml to override the default the file you create
  looks like this with your own values replaced.
  
 # awr_io_analyze.toml — threshold config for AWR I/O Analyzer
wait_pct = 10.0
io_latency_ms = 20.0
row_lock_pct = 3.0
gc_remote_pct = 2.0
io_request_rate =10000.0

Developed by Laurence Oberman, assisted by ChatGPT (OpenAI), 2025
");
    println!("Version {}",VERSION_NUMBER);
    process::exit(1);
}

/// Reads file into vector of lines
fn read_lines(path: &str) -> Vec<String> {
    fs::read_to_string(path)
        .expect("Failed to read file")
        .lines()
        .map(|s| s.to_string())
        .collect()
}

/// Extracts native AWR table
fn extract_native_table(lines: &[String], section_title: &str, max_gap: usize)
    -> Option<Vec<String>>
{
    let section_pat = Regex::new(section_title).unwrap();

let stop_patterns = vec![
    Regex::new(r"^Main Report").unwrap(),
    Regex::new(r"^Back to Top").unwrap(),
    Regex::new(r"^Wait Events Statistics").unwrap(),
    Regex::new(r"^Instance Activity").unwrap(),
    Regex::new(r"^SQL Statistics").unwrap(),
    Regex::new(r"^Undo Statistics").unwrap(),
    Regex::new(r"^Segment Statistics").unwrap(),
    Regex::new(r"^Library Cache Statistics").unwrap(),
    Regex::new(r"^Initialization Parameters").unwrap(),
    Regex::new(r"^ADDM Reports").unwrap(),
    Regex::new(r"^Top Process Types").unwrap(),
    Regex::new(r"^Service Statistics").unwrap(),
    Regex::new(r"^Service Wait Class Stats").unwrap(),
    // *** Add these lines ***
    Regex::new(r"^Wait Classes by Total Wait Time").unwrap(),
    Regex::new(r"^IO Profile").unwrap(),
];

    let mut start_idx = None;

    for (i, line) in lines.iter().enumerate() {
        if section_pat.is_match(line) {
            start_idx = Some(i);
            break;
        }
    }

    let start = start_idx?;
    let mut table = Vec::new();
    let mut started = false;
    let mut gap = 0;

    for l in &lines[start + 1..] {
        let trim = l.trim();

        if stop_patterns.iter().any(|p| p.is_match(trim)) {
            break;
        }

        if Regex::new(r"^\s*-\s+").unwrap().is_match(l)
            || Regex::new(r"^[A-Z][A-Za-z\s]+:$").unwrap().is_match(trim)
        {
            if started {
                break;
            } else {
                continue;
            }
        }

        if trim.is_empty() {
            if started {
                gap += 1;
                if gap >= max_gap {
                    break;
                }
            }
            continue;
        }

        if trim.chars().all(|c| c == '-' || c == '–' || c == '—') {
            if started {
                table.push(l.clone());
            }
            continue;
        }

        if !started {
            started = true;
        }

        table.push(l.clone());
        gap = 0;
    }

    if table.is_empty() {
        None
    } else {
        Some(table)
    }
}

/// Extracts % DB Time from table rows
fn extract_percent_from_wait_row(row: &str) -> Option<f64> {
    let parts: Vec<&str> = row.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    for i in (1..parts.len()).rev() {
        if parts[i].chars().all(|c| c.is_alphabetic() || c == '/') && i > 0 {
            if let Ok(v) = parts[i - 1].replace(',', "").parse::<f64>() {
                return Some(v);
            }
        }
    }
    None
}

/* ============================================================================
   NEW: LATENCY EXTRACTION (ms) FROM THE "Avg Wait" COLUMN
   Handles:
     212.99us  → 0.212ms
     9.27ms    → 9.27ms
     .99ms     → 0.99ms
     3252.59ms → 3252.59ms
   ============================================================================
*/
fn extract_latency_ms(row: &str) -> Option<f64> {
    let re = Regex::new(r"(\d*\.?\d+)(ms|us)").unwrap();

    if let Some(cap) = re.captures(row) {
        let val: f64 = cap[1].parse().ok()?;
        let unit = &cap[2];

        return Some(match unit {
            "us" => val / 1000.0, // convert microseconds → milliseconds
            "ms" => val,
            _ => return None,
        });
    }
    None
}

fn extract_event_name(row: &str) -> String {
    // Normalize all whitespace: tabs and weird unicode spaces from HTML->txt
    let mut clean = row.replace('\t', " ");
    for ws in ['\u{00A0}', '\u{2007}', '\u{202F}'] {
        clean = clean.replace(ws, " ");
    }

    // Split by *2 or more* spaces (AWR column boundary is usually 2+ spaces!)
    let re = Regex::new(r" {2,}").unwrap();
    let mut split = re.split(clean.trim());
    if let Some(event) = split.next() {
        if !event.trim().is_empty() {
            return event
                .trim()
                .to_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
                .replace("__", "_")
                .trim_matches('_')
                .to_string();
        }
    }

    // fallback: grab all up to first digit (start of "Waits" column)
    let mut event = String::new();
    for c in clean.chars() {
        if c.is_numeric() { break; }
        event.push(c);
    }
    if !event.trim().is_empty() {
        return event
            .trim()
            .to_lowercase()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
            .replace("__", "_")
            .trim_matches('_')
            .to_string();
    }

    "unknown".to_string()
}

/* ========================================================================
   ALERT LOGIC: Now expanded for nearly all Top 20 rules!
   ======================================================================== */

fn alert_on_fg_waits(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        // [RULE 4] General: high % wait time for any FG event
        if let Some(pct) = extract_percent_from_wait_row(row) {
            if pct > t.wait_pct {
                let event = extract_event_name(row);
                alerts.push(format!("🟠 High wait time for event '{}': {:.1}% of DB time.", event, pct));
            }
        }

        // [RULE 2] High I/O latency (any event)
        if let Some(lat) = extract_latency_ms(row) {
            let event = extract_event_name(row);
            if lat > t.io_latency_ms {
                alerts.push(format!(
                    "🔴 High I/O latency for '{}': {:.2}ms (> {}ms threshold).",
                    event, lat, t.io_latency_ms
                ));
            }
        }

        // [RULE 10] log file sync / parallel write
        if row.contains("log file sync") || row.contains("log file parallel write") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push(format!("🔴 Redo log bottleneck: '{}' {:.1}% of DB time.", extract_event_name(row), pct));
                }
            }
        }

        // [RULE 11] buffer busy waits
        if row.contains("buffer busy waits") {
            alerts.push("🟠 buffer busy waits — hot blocks likely.".into());
        }

        // [RULE 13] row lock contention
        if row.contains("row lock contention") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.row_lock_pct {
                    alerts.push(format!("🔴 Row lock contention: {:.1}% — investigate blocking.", pct));
                }
            }
        }

        // [RULE 14] GC remote, any "gc" event
        if row.to_lowercase().contains("gc") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.gc_remote_pct {
                    alerts.push(format!(
                        "🔴 Global Cache (RAC) event '{}': {:.1}% — possible RAC/interconnect issue.",
                        extract_event_name(row), pct
                    ));
                }
            }
        }

        // [RULE 15] Enqueue/contention (enq:)
        if row.contains("enq:") {
            alerts.push(format!("🟠 Contention: '{}' seen. Check blocking/locking.", extract_event_name(row)));
        }

        // [RULE 9] db file parallel read
        if row.contains("db file parallel read") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push(format!(
                        "🟠 High 'db file parallel read': {:.1}% — possible parallel I/O tuning needed.",
                        pct
                    ));
                }
            }
        }

        // [RULE 12] Temp I/O (direct path write temp)
        if row.contains("direct path write temp") || row.contains("direct path read temp") {
            alerts.push("🟡 Temp I/O — heavy temp usage detected.".into());
        }
    }
    alerts
}

/* ========================================================================
   NEW: Background Wait Events Section (add parsing if your reports have it)
   ======================================================================== */
fn alert_on_bg_waits(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        if row.contains("log file parallel write") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push("🟡 High background 'log file parallel write' — possible LGWR or storage bottleneck.".into());
                }
            }
        }
        if row.contains("db file parallel write") {
            alerts.push("🟠 Background 'db file parallel write' seen — possible checkpoint/backup or async I/O pressure.".into());
        }
        // ...add other background events here...
    }
    alerts
}

/* ========================================================================
   Wait Class Table Rules — mostly unchanged, but now flags more classes
   ======================================================================== */
fn alert_on_wait_classes(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        if row.contains("User I/O") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push("🟡 High User I/O class — DB is I/O-bound.".into());
                }
            }
        }
        if row.contains("Commit") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push("🟠 Commit wait class elevated — redo pressure.".into());
                }
            }
        }
        if row.contains("Concurrency") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.row_lock_pct {
                    alerts.push("🔴 High concurrency wait class — locking/contention suspected.".into());
                }
            }
        }
        // Add checks for System I/O, Configuration, Network, etc if desired
    }
    alerts
}

/* ========================================================================
   IO Profile: Add ratio logic, scattered vs sequential, and more
   ======================================================================== */
fn alert_on_io_profile(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();
    let num_re = Regex::new(r"(\d[\d,\.]+)").unwrap();

    let mut total_requests: Option<f64> = None;
    let mut read_reqs: Option<f64> = None;
    let mut write_reqs: Option<f64> = None;
    let mut read_mb: Option<f64> = None;
    let mut write_mb: Option<f64> = None;
    let mut scattered_reads: Option<f64> = None;
    let mut sequential_reads: Option<f64> = None;

    for l in table {
        // [RULE 1] Total requests per sec
        if l.contains("Total Requests:") {
            let vals: Vec<f64> = num_re.captures_iter(l)
                .filter_map(|c| c[1].replace(',', "").parse().ok())
                .collect();
            if let Some(first) = vals.first() {
                total_requests = Some(*first);
                if *first > t.io_request_rate {
                    alerts.push(format!("🟠 High I/O request rate: {} (> {} threshold).", first, t.io_request_rate));
                }
            }
        }
        // [RULE 8] Read vs Write requests
        if l.contains("Read Requests per Second") {
            read_reqs = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
        if l.contains("Write Requests per Second") {
            write_reqs = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
        // [RULE 17] Read/Write MB/s
        if l.contains("Read MB/sec") {
            read_mb = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
        if l.contains("Write MB/sec") {
            write_mb = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
        // [RULE 19] Scattered/Sequential reads
        if l.contains("db file scattered read") {
            scattered_reads = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
        if l.contains("db file sequential read") {
            sequential_reads = num_re.captures_iter(l)
                .next()
                .and_then(|c| c[1].replace(',', "").parse().ok());
        }
    }
    // [RULE 8] Write/Read ratio
    if let (Some(w), Some(r)) = (write_reqs, read_reqs) {
        if w > r * 2.0 {
            alerts.push(format!(
                "🟠 Write requests are more than 2x reads ({:.2} writes/sec vs {:.2} reads/sec). Check for redo/temp bottleneck.",
                w, r
            ));
        }
    }
    // [RULE 17] Throughput anomaly
    if let (Some(total), Some(rmb), Some(wmb)) = (total_requests, read_mb, write_mb) {
        let total_mb = rmb + wmb;
        if total > 0.0 && total_mb < 1.0 {
            alerts.push(format!(
                "🟡 High IOPS ({:.1}) but very low MB/sec ({:.2}). Many small I/Os? Check block size or inefficient access.",
                total, total_mb
            ));
        }
    }
    // [RULE 19] Scattered vs sequential
    if let (Some(sc), Some(seq)) = (scattered_reads, sequential_reads) {
        if sc > seq * 2.0 {
            alerts.push(format!(
                "🟠 'db file scattered read' >2x 'sequential read' ({:.2} vs {:.2}). Full table scans may be dominating.",
                sc, seq
            ));
        }
    }
    alerts
}

/* ========================================================================
   OUTPUT — Add new section for Background Waits if needed
   ======================================================================== */
fn print_table_with_alert<F>(
    lines: &[String],
    title_pat: &str,
    section_name: &str,
    alert_fn: F,
    thresholds: &AlertThresholds,
)
where
    F: Fn(&[String], &AlertThresholds) -> Vec<String>,
{
    println!("## {}\n", section_name);

    if let Some(table) = extract_native_table(lines, title_pat, 2) {
        for l in &table {
            println!("{}", l);
        }

        let alerts = alert_fn(&table, thresholds);

        if alerts.is_empty() {
            println!("\nNo immediate I/O issues flagged.\n");
        } else {
            println!("\n### 🚩 Analysis / Comments");
            for a in alerts {
                println!("- {}", a);
            }
            println!();
        }
    } else {
        println!("*No {} section found.*\n", section_name.to_lowercase());
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        usage();
    }
    println!("awr_io_analyze - Version {}",VERSION_NUMBER);
    let filename = &args[1];
    let config_path = if args.len() >= 3 { &args[2] } else { "awr_io_analyze.toml" };
    let thresholds = load_thresholds_from_file(config_path);
    let lines = read_lines(filename);

    println!("# AWR I/O Analysis for `{}`\n", filename);
    println!("**Thresholds: {:?}**\n", thresholds);

    print_table_with_alert(
        &lines,
        r"Top 10 Foreground Events by Total Wait Time",
        "Foreground Wait Events",
        alert_on_fg_waits,
        &thresholds,
    );
    // Optionally add Background Waits if your AWR has such a section:
    // print_table_with_alert(
    //     &lines,
    //     r"Top 10 Background Events by Total Wait Time",
    //     "Background Wait Events",
    //     alert_on_bg_waits,
    //     &thresholds,
    // );
    print_table_with_alert(
        &lines,
        r"Wait Classes by Total Wait Time",
        "Wait Classes",
        alert_on_wait_classes,
        &thresholds,
    );
    print_table_with_alert(
        &lines,
        r"IO Profile",
        "IO Profile",
        alert_on_io_profile,
        &thresholds,
    );

    println!("## Knowledge Base / Best Practices");
    println!("- log file sync / parallel write: redo bottleneck.");
    println!("- db file sequential read: random I/O slowness.");
    println!("- db file scattered read: full table scans.");
    println!("- buffer busy waits: hot blocks.");
    println!("- High User I/O: storage-bound workload.");
    println!("- High Write/Read ratio: redo, temp, or checkpoint pressure.");
    println!("- Low MB/s with high IOPS: small block size or inefficient SQL.");
    println!("- Always correlate waits with SQL + I/O subsystem.\n");
}

