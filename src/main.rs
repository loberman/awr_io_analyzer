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

Developed by Laurence Oberman, assisted by ChatGPT (OpenAI), 2025
");
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


/* -------------------- ALERT LOGIC -------------------- */

fn alert_on_fg_waits(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {

        // % wait time alerts
        if row.contains("log file sync") && row.contains("Commit") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push(format!(
                        "🔴 ALERT: High 'log file sync' {:.1}% — redo/commit bottleneck likely.",
                        pct
                    ));
                }
            }
        }

        if row.contains("db file sequential read") && row.contains("User I/O") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push(format!(
                        "🟠 NOTICE: High 'db file sequential read' {:.1}% — slow random I/O.",
                        pct
                    ));
                }
            }
        }

        // === AVG WAIT LATENCY CHECK (with FULL metric annotation) ===
        if let Some(lat) = extract_latency_ms(row) {
            if lat > t.io_latency_ms {

                let metric_full = extract_event_name(row);

                alerts.push(format!(
                    "🔴 ALERT: High I/O latency {:.2}ms (> {}ms threshold) <----- {}",
                    lat, t.io_latency_ms, metric_full
                ));
            }
        }


        // Row lock % (rare but let's keep it explicit)
        if row.contains("row lock contention") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.row_lock_pct {
                    alerts.push(format!(
                        "🔴 Row lock contention {:.1}% — investigate blocking.",
                        pct
                    ));
                }
            }
        }

        // GC Remote %
        if row.to_lowercase().contains("gc") && row.to_lowercase().contains("remote") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.gc_remote_pct {
                    alerts.push(format!(
                        "🔴 High GC remote {:.1}% — possible RAC interconnect issue.",
                        pct
                    ));
                }
            }
        }

        if row.contains("buffer busy waits") {
            alerts.push("🟠 NOTICE: buffer busy waits — hot blocks likely.".into());
        }

        if row.contains("direct path write temp") {
            alerts.push("🟡 Temp I/O — heavy temp usage.".into());
        }
    }

    alerts
}


fn alert_on_wait_classes(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        if row.contains("User I/O") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > t.wait_pct {
                    alerts.push("🟡 NOTICE: User I/O class high — DB is I/O-bound.".into());
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
    }

    alerts
}

fn alert_on_io_profile(table: &[String], t: &AlertThresholds) -> Vec<String> {
    let mut alerts = Vec::new();
    let num_re = Regex::new(r"(\d[\d,\.]+)").unwrap();

    for l in table {
        if l.contains("Total Requests:") {
            let vals: Vec<f64> = num_re.captures_iter(l)
                .filter_map(|c| c[1].replace(',', "").parse().ok())
                .collect();

            if let Some(first) = vals.first() {
                if *first > t.io_request_rate {
                    println!("DEBUG: io_request_rate value = {} check value = {}",*first,t.io_request_rate);
                    alerts.push("🟠 High I/O request rate.".into());
                }
            }
        }
    }
    alerts
}


/* -------------------- OUTPUT -------------------- */

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

    let filename = &args[1];
    //println!("DEBUG: args.len = {} config args = {}",args.len(),&args[2]);
    let config_path = if args.len() >= 3 { &args[2] } else { "awr_io_analyze.toml" };
    //println!("DEBUG: config_path = {}",config_path);

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
    println!("- log file sync: redo bottleneck.");
    println!("- db file sequential read: random I/O slowness.");
    println!("- buffer busy waits: hot blocks.");
    println!("- High User I/O: storage-bound workload.");
    println!("- Always correlate waits with SQL + I/O subsystem.\n");
}

