//! CLI configuration parsing and validation.

use std::ffi::{OsStr, OsString};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::{ArgAction, Parser};
use rand::distr::Alphanumeric;
use rand::{Rng, rng};

/// Command-line arguments accepted by the Dedux binary.
#[derive(Parser, Debug)]
#[command(
    name = "dedux",
    author,
    version,
    about = "Large scale text deduplication CLI",
    long_about = None
)]
pub struct Cli {
    /// Input text file path.
    pub input: PathBuf,

    /// Output directory for unique lines.
    #[arg(value_name = "OUTPUT_DIR")]
    pub output: Option<PathBuf>,

    /// Read buffer size in megabytes.
    #[arg(long = "read-buf-mb", default_value_t = 128)]
    pub read_buf_mb: usize,

    /// Chunk write buffer size in megabytes.
    #[arg(long = "segment-buf-mb", default_value_t = 64)]
    pub segment_buf_mb: usize,

    /// Final output write buffer size in megabytes.
    #[arg(long = "output-buf-mb", default_value_t = 128)]
    pub output_buf_mb: usize,

    /// Maximum in-memory footprint for a dedup chunk in gigabytes (0 = unlimited).
    #[arg(long = "chunk-mem-gb", default_value_t = 8)]
    pub chunk_mem_gb: usize,

    /// Number of worker threads to use for deduplication (0 = auto-detect).
    #[arg(long = "threads", default_value_t = 0)]
    pub threads: usize,

    /// Optional working directory for intermediate files.
    #[arg(long = "tmp-dir")]
    pub tmp_dir: Option<PathBuf>,

    /// Keep empty lines instead of skipping them.
    #[arg(long = "keep-empty", action = ArgAction::SetTrue, default_value_t = false)]
    pub keep_empty: bool,

    /// Disable trimming of trailing CR ("\r") when reading lines.
    #[arg(long = "no-trim-crlf", action = ArgAction::SetTrue, default_value_t = false)]
    pub no_trim_crlf: bool,
}

/// Fully resolved pipeline configuration derived from CLI flags.
#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub input: PathBuf,
    pub output: PathBuf,
    pub tmp_dir: PathBuf,
    pub read_buffer_bytes: usize,
    pub segment_buffer_bytes: usize,
    pub result_buffer_bytes: usize,
    pub chunk_memory_bytes: usize,
    pub worker_threads: usize,
    pub skip_empty: bool,
    pub trim_crlf: bool,
}

impl PipelineConfig {
    /// Translate parsed CLI arguments into a validated configuration.
    pub fn from_cli(cli: Cli) -> Result<Self> {
        let Cli {
            input,
            output,
            read_buf_mb,
            segment_buf_mb,
            output_buf_mb,
            chunk_mem_gb,
            threads,
            tmp_dir,
            keep_empty,
            no_trim_crlf,
        } = cli;

        let input_path = input;
        let input = input_path
            .canonicalize()
            .with_context(|| format!("failed to resolve input path: {}", input_path.display()))?;
        if !input.is_file() {
            return Err(anyhow!("input path is not a file: {}", input.display()));
        }

        let output_dir = match output {
            Some(dir) => validate_output_directory(dir)?,
            None => {
                let parent = input.parent().map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from("."));
                validate_output_directory(parent)?
            }
        };
        let output = output_dir.join(default_output_file_name(&input));

        if output == input {
            return Err(anyhow!("output path must differ from input path"));
        }

        let tmp_dir = match tmp_dir {
            Some(path) => path,
            None => default_tmp_dir(&output),
        };

        let read_buffer_bytes = mb_to_bytes(read_buf_mb)?;
        let segment_buffer_bytes = mb_to_bytes(segment_buf_mb)?;
        let result_buffer_bytes = mb_to_bytes(output_buf_mb)?;
        let chunk_memory_bytes = gb_to_bytes(chunk_mem_gb)?;
        let worker_threads = if threads == 0 { num_cpus::get() } else { threads };
        if worker_threads == 0 {
            return Err(anyhow!("thread count must be at least 1"));
        }
        let skip_empty = !keep_empty;
        let trim_crlf = !no_trim_crlf;

        Ok(Self {
            input,
            output,
            tmp_dir,
            read_buffer_bytes,
            segment_buffer_bytes,
            result_buffer_bytes,
            chunk_memory_bytes,
            worker_threads,
            skip_empty,
            trim_crlf,
        })
    }
}

/// Ensure the provided output directory exists, is writable, and return its
/// canonical path.
fn validate_output_directory(dir: PathBuf) -> Result<PathBuf> {
    let metadata = fs::metadata(&dir).with_context(|| format!("output directory does not exist: {}", dir.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!("output path is not a directory: {}", dir.display()));
    }

    let test_path = dir.join(".dedux_perm_test");
    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&test_path)
        .with_context(|| format!("output directory is not writable: {}", dir.display()))?;
    let _ = fs::remove_file(&test_path);

    dir.canonicalize()
        .with_context(|| format!("failed to resolve output directory: {}", dir.display()))
}

/// Construct a default output filename based on the input path, preserving
/// extensions when possible.
fn default_output_file_name(input: &Path) -> PathBuf {
    let stem = input.file_stem().unwrap_or_else(|| OsStr::new("dedux_output"));
    let mut name: OsString = stem.to_os_string();
    name.push(".deduped");

    if let Some(ext) = input.extension() {
        let mut with_ext = name;
        with_ext.push(OsStr::new("."));
        with_ext.push(ext);
        PathBuf::from(with_ext)
    } else {
        PathBuf::from(name)
    }
}

/// Generate a random temporary directory alongside the output file.
fn default_tmp_dir(output: &Path) -> PathBuf {
    let parent = output.parent().unwrap_or_else(|| Path::new("."));
    let mut rng = rng();
    let random_suffix: String = (&mut rng).sample_iter(&Alphanumeric).take(12).map(char::from).collect();
    parent.join(format!(".dedux_tmp_{}", random_suffix))
}

/// Convert megabytes to bytes, guarding against overflow.
fn mb_to_bytes(value: usize) -> Result<usize> {
    value
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("value too large: {} MB", value))
}

/// Convert gigabytes to bytes, guarding against overflow.
fn gb_to_bytes(value: usize) -> Result<usize> {
    value
        .checked_mul(1024 * 1024 * 1024)
        .ok_or_else(|| anyhow!("value too large: {} GB", value))
}
