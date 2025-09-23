//! Minimal telemetry helpers for surface-level progress reporting.

use std::time::{Duration, Instant};

/// Lightweight helper that timestamps stage and progress updates.
#[derive(Clone, Copy)]
pub struct Telemetry {
    start: Instant,
}

impl Telemetry {
    /// Create a new telemetry instance anchored at the current time.
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    /// Record a stage transition message.
    pub fn stage(&self, message: &str) {
        self.emit(message);
    }

    /// Record an incremental progress update.
    pub fn progress(&self, message: &str) {
        self.emit(message);
    }

    fn emit(&self, message: &str) {
        println!("[{}] {message}", format_elapsed(self.start.elapsed()));
    }
}

fn format_elapsed(duration: Duration) -> String {
    let secs = duration.as_secs();
    let millis = duration.subsec_millis();
    format!("{:02}:{:02}.{:03}", (secs / 60) % 60, secs % 60, millis)
}

/// Format a byte counter using IEC units up to tebibytes.
pub fn format_bytes(value: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut val = value as f64;
    let mut unit = 0;
    while val >= 1024.0 && unit < UNITS.len() - 1 {
        val /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{} {}", value, UNITS[unit])
    } else {
        format!("{:.2} {}", val, UNITS[unit])
    }
}
