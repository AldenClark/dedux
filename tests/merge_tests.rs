use dedux::chunk_deduper::Segment;
use dedux::merge::merge_segments;
use dedux::telemetry::Telemetry;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::collections::BTreeSet;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use utils::generate_random_password_lines_with_rng;

mod utils;

fn unique_temp_dir() -> PathBuf {
    let base = std::env::temp_dir();
    let pid = std::process::id();
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    base.join(format!("dedux_merge_test_{pid}_{ts}"))
}

#[test]
fn merge_segments_respects_custom_fd_limit() {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).unwrap();

    let segments_data = vec![
        (&["apple", "carrot"][..]),
        (&["banana", "carrot"][..]),
        (&["banana", "pear"][..]),
        (&["orange", "plum"][..]),
        (&["apple", "watermelon"][..]),
    ];

    let mut segments = Vec::new();
    for (idx, lines) in segments_data.into_iter().enumerate() {
        let path = temp_dir.join(format!("segment_{idx:02}.run"));
        let mut file = File::create(&path).unwrap();
        for line in lines {
            writeln!(file, "{line}").unwrap();
        }
        let bytes = fs::metadata(&path).unwrap().len();
        segments.push(Segment { path, bytes });
    }

    let output_path = temp_dir.join("final.txt");
    let key = "DEDUX_MAX_OPEN_FILES";
    let previous = env::var(key).ok();
    unsafe {
        env::set_var(key, "20");
    }

    let stats = merge_segments(&segments, &output_path, 16 * 1024, &Telemetry::new()).unwrap();

    match previous {
        Some(value) => unsafe {
            env::set_var(key, value);
        },
        None => unsafe {
            env::remove_var(key);
        },
    }

    let contents = fs::read_to_string(&output_path).unwrap();
    let expected = "apple\nbanana\ncarrot\norange\npear\nplum\nwatermelon\n";
    assert_eq!(contents, expected);
    assert_eq!(stats.unique_lines, 7);
    assert_eq!(stats.segments, 5);

    fs::remove_dir_all(&temp_dir).unwrap();
}

#[test]
fn merge_segments_handles_randomized_overlap() {
    let temp_dir = unique_temp_dir();
    fs::create_dir_all(&temp_dir).unwrap();

    let mut rng = StdRng::seed_from_u64(12345);
    let dataset = generate_random_password_lines_with_rng(&mut rng, 24_000, 8..=32);
    let as_strings: Vec<String> = dataset.into_iter().map(|line| String::from_utf8(line).unwrap()).collect();

    let segments_specs: Vec<Vec<String>> = vec![
        as_strings[0..120].to_vec(),
        as_strings[60..180].to_vec(),
        as_strings[120..240].to_vec(),
    ];

    let mut segments = Vec::new();
    let mut expected = BTreeSet::new();
    for (idx, mut lines) in segments_specs.into_iter().enumerate() {
        lines.sort();
        lines.dedup();
        expected.extend(lines.iter().cloned());

        let path = temp_dir.join(format!("random_segment_{idx:02}.run"));
        let mut file = File::create(&path).unwrap();
        for line in &lines {
            writeln!(file, "{line}").unwrap();
        }
        let bytes = fs::metadata(&path).unwrap().len();
        segments.push(Segment { path, bytes });
    }

    let output_path = temp_dir.join("random_final.txt");
    let stats = merge_segments(&segments, &output_path, 32 * 1024, &Telemetry::new()).unwrap();

    let merged_lines: Vec<String> = fs::read_to_string(&output_path)
        .unwrap()
        .lines()
        .map(|line| line.to_string())
        .collect();

    let expected: Vec<String> = expected.into_iter().collect();

    assert_eq!(merged_lines, expected);
    assert_eq!(stats.unique_lines, expected.len() as u64);
    assert_eq!(stats.segments, 3);

    fs::remove_dir_all(&temp_dir).unwrap();
}
