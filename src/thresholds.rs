/*!
 * thresholds.rs — Configurable alert thresholds for awr_io_analyze
 *
 * Allows tuning of all major analysis limits from a TOML config file,
 * so you do NOT have to recompile just to change an alert value.
 *
 * Co-developed by Laurence Oberman and ChatGPT (OpenAI), 2025.
 * License: GPLv3+
 */

use std::fs;

/// All configurable thresholds for AWR analysis.
/// Add more fields here for new alert types.
#[derive(Debug, Clone)]
pub struct AlertThresholds {
    pub wait_pct: f64,        // % DB Time for waits (default: 10.0)
    pub io_latency_ms: f64,   // I/O latency in ms (default: 20.0)
    pub row_lock_pct: f64,    // Row lock contention % (default: 3.0)
    pub gc_remote_pct: f64,   // GC remote transfer % (default: 2.0)
    // Add more thresholds below as needed!
    pub io_request_rate: f64, // i/o request rate from io_profile view
}

/// Defaults used if no config file or missing values.
impl Default for AlertThresholds {
    fn default() -> Self {
        AlertThresholds {
            wait_pct: 10.0,
            io_latency_ms: 20.0,
            row_lock_pct: 3.0,
            gc_remote_pct: 2.0,
            io_request_rate: 10_000.0,
        }
    }
}

/// Loads thresholds from a TOML config file (if present).
/// Falls back to defaults for missing keys or missing file.
///
/// Example TOML:
/// ```toml
/// wait_pct = 10.0
/// io_latency_ms = 20.0
/// row_lock_pct = 3.0
/// gc_remote_pct = 2.0
/// ```
pub fn load_thresholds_from_file(path: &str) -> AlertThresholds {
    let contents = fs::read_to_string(path);
    let mut t = AlertThresholds::default();

    if let Ok(data) = contents {
        for line in data.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.split('=').map(|s| s.trim()).collect();
            if parts.len() != 2 { continue; }

            match parts[0] {
                "wait_pct" =>    t.wait_pct = parts[1].parse().unwrap_or(t.wait_pct),
                "io_latency_ms" => t.io_latency_ms = parts[1].parse().unwrap_or(t.io_latency_ms),
                "row_lock_pct" => t.row_lock_pct = parts[1].parse().unwrap_or(t.row_lock_pct),
                "gc_remote_pct" => t.gc_remote_pct = parts[1].parse().unwrap_or(t.gc_remote_pct),
                "io_request_rate" => t.io_request_rate = parts[1].parse().unwrap_or(t.io_request_rate),
                _ => {},
            }
        }
    }
    t
}

