use dedux::config::{Cli, PipelineConfig};
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir() -> PathBuf {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    base.join(format!("dedux_config_test_{pid}_{ts}"))
}

fn base_cli(input: PathBuf) -> Cli {
    Cli {
        input,
        output: None,
        read_buf_mb: 128,
        segment_buf_mb: 64,
        output_buf_mb: 128,
        chunk_mem_gb: 8,
        threads: 0,
        tmp_dir: None,
        keep_empty: false,
        no_trim_crlf: false,
    }
}

#[test]
fn default_output_keeps_extension() {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).unwrap();
    let input_path = temp_dir.join("example.txt");
    File::create(&input_path).unwrap();

    let cli = base_cli(input_path.clone());
    let config = PipelineConfig::from_cli(cli).unwrap();

    assert_eq!(config.output.file_name().unwrap(), "example.deduped.txt");

    fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn default_output_handles_multi_part_extensions() {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).unwrap();
    let input_path = temp_dir.join("archive.tar.gz");
    File::create(&input_path).unwrap();

    let cli = base_cli(input_path.clone());
    let config = PipelineConfig::from_cli(cli).unwrap();

    assert_eq!(config.output.file_name().unwrap(), "archive.tar.deduped.gz");

    fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn default_output_without_extension() {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).unwrap();
    let input_path = temp_dir.join("data");
    File::create(&input_path).unwrap();

    let cli = base_cli(input_path.clone());
    let config = PipelineConfig::from_cli(cli).unwrap();

    assert_eq!(config.output.file_name().unwrap(), "data.deduped");

    fs::remove_dir_all(&temp_dir).unwrap();
}
