/*!
 * awr_io_analyze.rs — Oracle AWR I/O Analyzer (Rust)
 *
 * Quickly extract and analyze Oracle AWR (Automatic Workload Repository) I/O-related tables
 * directly from a standard AWR text report, outputting the original table along with
 * actionable expert comments and problem alerts underneath.
 *
 * Usage:
 *   awr_io_analyze <awrrpt_xxx.txt>
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

use regex::Regex;
use std::env;
use std::fs;
use std::process;

/// Print usage and exit. (If called, process will not return!)
fn usage() {
    eprintln!("
Oracle AWR I/O Analyzer (Rust)
------------------------------

Usage:
  awr_io_analyze <awrrpt_xxx.txt>

Example:
  awr_io_analyze awrrpt_1_67450_67453_RMS.html.txt

- Reads an AWR text report and prints I/O tables with alerts/comments
- Output: Original AWR tables + expert analysis under each table

Developed by Laurence Oberman, assisted by ChatGPT (OpenAI), 2025
");
    process::exit(1);
}

/// Read file into vector of String, one per line.
///
/// # Arguments
/// * `path` — Path to the file to read
fn read_lines(path: &str) -> Vec<String> {
    fs::read_to_string(path)
        .expect("Failed to read file")
        .lines()
        .map(|s| s.to_string())
        .collect()
}

/// Extract a native AWR table (exactly as printed) into Vec<String>.
///
/// Searches for the section header, skips navigation/menu lines,
/// and captures all table lines until either too many consecutive
/// blanks or a known unrelated section is encountered.
///
/// # Arguments
/// * `lines` — Slice of the report as strings
/// * `section_title` — Regex pattern for the table section header
/// * `max_gap` — How many consecutive blanks end a table
fn extract_native_table(lines: &[String], section_title: &str, max_gap: usize)
    -> Option<Vec<String>>
{
    let section_pat = Regex::new(section_title).unwrap();

    // List of patterns that mark a *new unrelated section* (not part of the table)
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
    ];

    // 1. Find the start of the section
    let mut start_idx = None;
    for (i, line) in lines.iter().enumerate() {
        if section_pat.is_match(line) {
            start_idx = Some(i);
            break;
        }
    }
    let start = start_idx?;

    // 2. Scan forward to extract table lines, handling blanks and dash-lines
    let mut table: Vec<String> = Vec::new();
    let mut started = false;
    let mut gap = 0;

    for l in &lines[start + 1..] {
        let trim = l.trim();

        // End if we hit an unrelated section header
        if stop_patterns.iter().any(|p| p.is_match(trim)) {
            break;
        }

        // Skip navigation menu lines (e.g., "- Wait Events", or "Report Summary:")
        if Regex::new(r"^\s*-\s+").unwrap().is_match(l)
            || Regex::new(r"^[A-Z][A-Za-z\s]+:$").unwrap().is_match(trim)
        {
            if started {
                break;
            } else {
                continue;
            }
        }

        // Handle blank lines (for end of table detection)
        if trim.is_empty() {
            if started {
                gap += 1;
                if gap >= max_gap {
                    break;
                }
            }
            continue;
        }

        // Horizontal dashed lines are part of table formatting
        if trim.chars().all(|c| c == '-' || c == '–' || c == '—') {
            if started {
                table.push(l.clone());
            }
            continue;
        }

        // Start table after first non-blank, non-nav line
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

/// Extracts the percentage column from a wait-event table row,
/// just before the "Wait Class" (last word).
///
/// Example row:
///   "log file sync   232,142   2151.6   9.27ms   7.0 Commit"
/// Will find and parse "7.0" (the percent) as f64.
///
/// Returns None if not found.
fn extract_percent_from_wait_row(row: &str) -> Option<f64> {
    let parts: Vec<&str> = row.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }
    // Walk backwards: if current is alpha and previous parses as float, that's the percent
    for i in (1..parts.len()).rev() {
        if parts[i].chars().all(|c| c.is_alphabetic() || c == '/') && i > 0 {
            if let Ok(val) = parts[i - 1].replace(',', "").parse::<f64>() {
                return Some(val);
            }
        }
    }
    None
}

/* -------------------- ALERT LOGIC -------------------- */

/// Alert logic for Foreground Wait Events table.
/// Flags high log file sync, db file sequential read, buffer busy waits, etc.
fn alert_on_fg_waits(table: &[String]) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        if row.contains("log file sync") && row.contains("Commit") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > 5.0 {
                    alerts.push(format!(
                        "🔴 ALERT: High 'log file sync' {:.1}% — redo/commit bottleneck likely.",
                        pct
                    ));
                }
            }
        }
        if row.contains("db file sequential read") && row.contains("User I/O") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > 5.0 {
                    alerts.push(format!(
                        "🟠 NOTICE: High 'db file sequential read' {:.1}% — slow random I/O.",
                        pct
                    ));
                }
            }
        }
        if row.contains("buffer busy waits") {
            alerts.push("🟠 NOTICE: 'buffer busy waits' — hot blocks / buffer cache contention."
                .to_string());
        }

        if row.contains("direct path write temp") {
            alerts.push("🟡 Temp I/O — check temp tablespace usage.".to_string());
        }
    }

    alerts
}

/// Alert logic for Wait Classes table.
/// Flags high "User I/O" (DB is I/O bound), high "Commit" class, etc.
fn alert_on_wait_classes(table: &[String]) -> Vec<String> {
    let mut alerts = Vec::new();

    for row in table {
        if row.contains("User I/O") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > 15.0 {
                    alerts.push("🟡 NOTICE: User I/O wait class high — database is I/O-bound.".into());
                }
            }
        }
        if row.contains("Commit") {
            if let Some(pct) = extract_percent_from_wait_row(row) {
                if pct > 5.0 {
                    alerts.push("🟠 Commit class high — commit rate or redo bottleneck.".into());
                }
            }
        }
    }

    alerts
}

/// Alert logic for IO Profile table.
/// Highlights if total I/O request rate is unusually high.
fn alert_on_io_profile(table: &[String]) -> Vec<String> {
    let mut alerts = Vec::new();
    let num_re = Regex::new(r"(\d[\d,\.]*)").unwrap();

    for l in table {
        if l.contains("Total Requests:") {
            let nums: Vec<f64> = num_re
                .captures_iter(l)
                .filter_map(|c| c[1].replace(',', "").parse().ok())
                .collect();

            if let Some(v) = nums.first() {
                if *v > 10_000.0 {
                    alerts.push("🟠 Very high I/O request rate.".into());
                }
            }
        }
    }

    alerts
}

/* -------------------- OUTPUT -------------------- */

/// Print a table with its section heading, then analyze and print alerts underneath.
/// If section not found, prints a notice.
///
/// # Arguments
/// * `lines`      — Full report as slice of lines
/// * `title_pat`  — Regex for table header to extract
/// * `section_name` — Label for section heading in output
/// * `alert_fn`   — Function to call for alert logic
fn print_table_with_alert(
    lines: &[String],
    title_pat: &str,
    section_name: &str,
    alert_fn: fn(&[String]) -> Vec<String>,
) {
    println!("## {}\n", section_name);

    if let Some(table) = extract_native_table(lines, title_pat, 2) {
        for l in &table {
            println!("{}", l);
        }

        let alerts = alert_fn(&table);
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

/// Entry point: parses command line, reads file, and runs all analysis/reporting.
fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        usage();
    }

    let filename = &args[1];
    let lines = read_lines(filename);

    println!("# AWR I/O Analysis for `{}`\n", filename);

    print_table_with_alert(
        &lines,
        r"Top 10 Foreground Events by Total Wait Time",
        "Foreground Wait Events",
        alert_on_fg_waits,
    );

    print_table_with_alert(
        &lines,
        r"Wait Classes by Total Wait Time",
        "Wait Classes",
        alert_on_wait_classes,
    );

    print_table_with_alert(
        &lines,
        r"IO Profile",
        "IO Profile",
        alert_on_io_profile,
    );

    println!("## Knowledge Base / Best Practices");
    println!("- log file sync: redo log bottleneck.");
    println!("- db file sequential read: random I/O latency.");
    println!("- buffer busy waits: hot blocks.");
    println!("- High User I/O: DB is storage-bound.");
    println!("- Always correlate waits with SQL + storage behavior.\n");
}

