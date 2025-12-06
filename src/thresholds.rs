// thresholds.rs
// Thresholds struct and config loader for awr_io_analyze
//
// Co-developed by Laurence Oberman and ChatGPT (OpenAI), 2025.
// License: GPLv3+

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AlertThresholds {
    pub wait_pct: f64,        // e.g. % DB time for waits (default: 10.0)
    pub io_latency_ms: f64,   // e.g. I/O latency in ms (default: 20.0)
    pub row_lock_pct: f64,    // % DB time for row lock contention (default: 1.0)
    pub gc_remote_pct: f64,   // % DB time for GC remote transfer waits (default: 1.0)
}

impl Default for AlertThresholds {
    fn default() -> Self {
        AlertThresholds {
            wait_pct: 10.0,
            io_latency_ms: 20.0,
            row_lock_pct: 1.0,
            gc_remote_pct: 1.0,
        }
    }
}

/// Loads thresholds from TOML config. Falls back to defaults if file missing or invalid.
pub fn load_thresholds_from_file(path: &str) -> AlertThresholds {
    let data = std::fs::read_to_string(path);
    match data {
        Ok(txt) => toml::from_str(&txt).unwrap_or_else(|e| {
            eprintln!("Config parse error ({}), using defaults.", e);
            AlertThresholds::default()
        }),
        Err(_) => {
            eprintln!("No config found at {}, using defaults.", path);
            AlertThresholds::default()
        }
    }
}

