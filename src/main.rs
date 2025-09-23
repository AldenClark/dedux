//! CLI entry point that orchestrates the chunk build and merge stages for
//! large-scale text deduplication.

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use dedux::chunk_deduper::{ChunkBuildStats, Segment, build_segments};
use dedux::config::{Cli, PipelineConfig};
use dedux::merge::{MergeStats, merge_segments};
use dedux::telemetry::{Telemetry, format_bytes};
use std::fs;
use std::path::{Path, PathBuf};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = PipelineConfig::from_cli(cli)?;
    run_pipeline(&config)
}

/// Execute the end-to-end deduplication pipeline for a single CLI invocation.
fn run_pipeline(config: &PipelineConfig) -> Result<()> {
    let telemetry = Telemetry::new();
    telemetry.stage("Starting Dedux CLI pipeline");

    prepare_tmp_dir(config)?;
    let _tmp_dir_guard = TempDirGuard::new(config.tmp_dir.clone());

    let segments_dir = config.tmp_dir.join("segments");
    telemetry.stage("Phase 1: building sorted unique segments");
    let (segments, build_stats) = build_segments(config, &segments_dir, &telemetry)?;
    report_build_stats(&telemetry, &build_stats);

    telemetry.stage("Phase 2: merging segments into final output");
    let merge_stats = merge_segments(
        &segments,
        &config.output,
        config.result_buffer_bytes,
        &telemetry,
    )?;
    report_merge_stats(&telemetry, &merge_stats);

    cleanup_segments(&segments, &segments_dir);

    telemetry.stage("Deduplication finished");
    Ok(())
}

/// Ensure the temporary working directory is available and empty before
/// starting the pipeline.
fn prepare_tmp_dir(config: &PipelineConfig) -> Result<()> {
    if config.tmp_dir.exists() {
        return Err(anyhow!(
            "temporary directory already exists: {} (remove it before running)",
            config.tmp_dir.display()
        ));
    }
    fs::create_dir_all(&config.tmp_dir).with_context(|| {
        format!(
            "failed to create temporary directory: {}",
            config.tmp_dir.display()
        )
    })
}

/// Emit chunk-building statistics once segment construction completes.
fn report_build_stats(telemetry: &Telemetry, stats: &ChunkBuildStats) {
    telemetry.stage(&format!(
        "Segment build complete: {} segments, {} input lines, {} unique lines, {} written",
        stats.segments,
        stats.total_input_lines,
        stats.unique_lines,
        format_bytes(stats.bytes_written)
    ));
}

/// Emit merge statistics for the final deduplicated output.
fn report_merge_stats(telemetry: &Telemetry, stats: &MergeStats) {
    telemetry.stage(&format!(
        "Merge complete: {} segments merged, {} unique lines emitted, {} written",
        stats.segments,
        stats.unique_lines,
        format_bytes(stats.total_bytes)
    ));
}

/// Best-effort removal of temporary segment files once they are no longer
/// needed.
fn cleanup_segments(segments: &[Segment], segments_dir: &Path) {
    for segment in segments {
        let _ = fs::remove_file(&segment.path);
    }
    if segments_dir.exists() {
        let _ = fs::remove_dir_all(segments_dir);
    }
}

/// Drop guard that removes the pipeline's temporary directory on scope exit.
struct TempDirGuard {
    path: PathBuf,
}

impl TempDirGuard {
    /// Create a guard for the provided temporary directory path.
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for TempDirGuard {
    /// Attempt to remove the temporary directory, logging a warning if cleanup
    /// fails. The guard is tolerant of concurrent manual deletions.
    fn drop(&mut self) {
        if !self.path.exists() {
            return;
        }
        if let Err(err) = fs::remove_dir_all(&self.path) {
            eprintln!(
                "warning: failed to clean temporary directory {}: {err}",
                self.path.display()
            );
        }
    }
}
