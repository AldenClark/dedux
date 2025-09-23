use dedux::chunk_deduper::build_segments;
use dedux::config::PipelineConfig;
use dedux::merge::merge_segments;
use dedux::telemetry::Telemetry;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use utils::generate_random_password_dataset_to_disk;

mod utils;

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    base.join(format!("{prefix}_{pid}_{ts}"))
}

fn run_dedupe_scenario(label: &str, total_lines: usize, unique_pool: usize, chunk_memory_bytes: usize) {
    let temp_dir = unique_temp_dir("dedux_pipeline_test");
    fs::create_dir_all(&temp_dir).unwrap();

    let mut rng = StdRng::seed_from_u64(0xDED0_5000 + total_lines as u64 + unique_pool as u64);
    let dataset =
        generate_random_password_dataset_to_disk(&mut rng, &temp_dir, &format!("{label}_input.txt"), total_lines, unique_pool, 8..=32)
            .unwrap();

    let output_path = temp_dir.join(format!("{label}_output.txt"));
    let segments_dir = temp_dir.join("segments");
    let telemetry = Telemetry::new();

    let config = PipelineConfig {
        input: dataset.path.clone(),
        output: output_path.clone(),
        tmp_dir: temp_dir.join("tmp"),
        read_buffer_bytes: 256 * 1024,
        segment_buffer_bytes: 256 * 1024,
        result_buffer_bytes: 256 * 1024,
        chunk_memory_bytes,
        worker_threads: 2,
        skip_empty: true,
        trim_crlf: true,
    };

    let chunk_start = Instant::now();
    let (segments, chunk_stats) = build_segments(&config, &segments_dir, &telemetry).expect("failed to build segments");
    let chunk_duration = chunk_start.elapsed();

    assert_eq!(chunk_stats.total_input_lines as usize, total_lines);
    assert_eq!(chunk_stats.unique_lines as usize, dataset.unique_line_count());

    let merge_start = Instant::now();
    let merge_stats = merge_segments(&segments, &output_path, config.result_buffer_bytes, &telemetry).expect("failed to merge segments");
    let merge_duration = merge_start.elapsed();

    assert_eq!(merge_stats.unique_lines as usize, dataset.unique_line_count());

    let chunk_secs = chunk_duration.as_secs_f64();
    let merge_secs = merge_duration.as_secs_f64();
    let deduped = chunk_stats.unique_lines;
    let duplicates = chunk_stats.total_input_lines - deduped;
    let chunk_throughput = if chunk_secs > 0.0 {
        chunk_stats.total_input_lines as f64 / chunk_secs
    } else {
        0.0
    };
    let merge_throughput = if merge_secs > 0.0 { deduped as f64 / merge_secs } else { 0.0 };

    println!(
        "[{label}] lines={} unique={} duplicates={} segments={} chunk_bytes={} merge_bytes={} chunk_time={:.4}s merge_time={:.4}s chunk_throughput={:.2} lines/s merge_throughput={:.2} lines/s",
        chunk_stats.total_input_lines,
        deduped,
        duplicates,
        chunk_stats.segments,
        chunk_stats.bytes_written,
        merge_stats.total_bytes,
        chunk_secs,
        merge_secs,
        chunk_throughput,
        merge_throughput,
    );

    let expected: BTreeSet<String> = dataset
        .unique_values
        .iter()
        .map(|value| String::from_utf8(value.clone()).unwrap())
        .collect();

    let output_data = fs::read_to_string(&output_path).expect("failed to read output");
    let actual: BTreeSet<String> = output_data.lines().map(|line| line.to_string()).collect();

    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual, expected);

    for segment in segments {
        let _ = fs::remove_file(segment.path);
    }
    dataset.cleanup();
    let _ = fs::remove_file(&output_path);
    let _ = fs::remove_dir_all(&segments_dir);
    let _ = fs::remove_dir_all(&config.tmp_dir);
    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn dedupe_pipeline_small_dataset_metrics() {
    run_dedupe_scenario("small", 500_000, 1_000, 2 * 1024 * 1024);
}

#[test]
fn dedupe_pipeline_medium_dataset_metrics() {
    run_dedupe_scenario("medium", 25_000_000, 3_000, 4 * 1024 * 1024);
}

#[test]
fn dedupe_pipeline_large_dataset_metrics() {
    run_dedupe_scenario("large", 10_000_000_000, 5_000, 6 * 1024 * 1024);
}
