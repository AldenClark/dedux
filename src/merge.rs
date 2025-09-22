//! Merge on-disk segment runs into the final deduplicated output file.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rayon::prelude::*;

use crate::chunk_deduper::Segment;
use crate::telemetry::Telemetry;

const DEFAULT_MAX_OPEN_FILES: usize = 512;
const FD_RESERVE: usize = 32;
const MIN_MERGE_FAN_IN: usize = 2;
const MAX_MERGE_FAN_IN: usize = 4096;
const MERGE_PROGRESS_LINE_INTERVAL: u64 = 1_000_000;
const MERGE_READER_BUFFER_MIN: usize = 64 * 1024;
const MERGE_READER_BUFFER_CAP: usize = 2 * 1024 * 1024;

/// Metrics gathered while merging segments.
#[derive(Debug, Clone)]
pub struct MergeStats {
    pub segments: usize,
    pub unique_lines: u64,
    pub total_bytes: u64,
}

/// Merge the provided segments into a single deduplicated output file,
/// performing multi-pass merging when the fan-in must be reduced.
pub fn merge_segments(
    segments: &[Segment],
    output_path: &Path,
    buffer_bytes: usize,
    telemetry: &Telemetry,
) -> Result<MergeStats> {
    let initial_count = segments.len();
    let max_fan_in = max_merge_fan_in();

    telemetry.progress(&format!(
        "Preparing to merge {initial_count} segments (fan-in limit {max_fan_in})"
    ));

    if initial_count == 0 {
        let mut stats = merge_into_path(segments, output_path, buffer_bytes, telemetry, true)?;
        stats.segments = 0;
        return Ok(stats);
    }

    if initial_count <= max_fan_in {
        let mut stats = merge_into_path(segments, output_path, buffer_bytes, telemetry, true)?;
        stats.segments = initial_count;
        return Ok(stats);
    }

    let scratch_dir = determine_scratch_dir(segments, output_path);
    fs::create_dir_all(&scratch_dir).with_context(|| {
        format!(
            "failed to prepare merge scratch directory: {}",
            scratch_dir.display()
        )
    })?;

    let mut current: Vec<Segment> = segments.to_vec();
    let mut pass = 0usize;

    while current.len() > max_fan_in {
        // Clone into owned batches to release files after each merge.
        let batches: Vec<(usize, Vec<Segment>)> = current
            .chunks(max_fan_in)
            .enumerate()
            .map(|(chunk_idx, chunk)| (chunk_idx, chunk.to_vec()))
            .collect();

        let mut merged_segments = batches
            .into_par_iter()
            .map(|(chunk_idx, batch)| -> Result<(usize, Segment)> {
                let temp_path =
                    scratch_dir.join(format!("merge_pass{:02}_chunk{:04}.run", pass, chunk_idx));
                let stats = merge_into_path(&batch, &temp_path, buffer_bytes, telemetry, false)?;
                let merged_segment = Segment {
                    path: temp_path,
                    bytes: stats.total_bytes,
                };

                for segment in batch {
                    let _ = fs::remove_file(&segment.path);
                }

                Ok((chunk_idx, merged_segment))
            })
            .collect::<Result<Vec<_>>>()?;

        merged_segments.sort_by_key(|(chunk_idx, _)| *chunk_idx);

        let mut next_round = Vec::with_capacity(merged_segments.len());
        next_round.extend(merged_segments.into_iter().map(|(_, segment)| segment));

        current = next_round;
        pass += 1;
        telemetry.progress(&format!(
            "Merge pass {pass} complete; {} intermediate segments remain",
            current.len()
        ));
    }

    let mut final_stats = merge_into_path(&current, output_path, buffer_bytes, telemetry, true)?;
    final_stats.segments = initial_count;

    for segment in current {
        let _ = fs::remove_file(&segment.path);
    }

    Ok(final_stats)
}

/// Merge a set of sorted segments into the specified output path. When
/// `emit_progress` is `true` the caller receives telemetry updates.
fn merge_into_path(
    segments: &[Segment],
    output_path: &Path,
    buffer_bytes: usize,
    telemetry: &Telemetry,
    emit_progress: bool,
) -> Result<MergeStats> {
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory: {}", parent.display()))?;
    }

    let file = File::create(output_path)
        .with_context(|| format!("failed to create output file: {}", output_path.display()))?;
    let buffer_size = buffer_bytes.max(64 * 1024);
    let mut writer = BufWriter::with_capacity(buffer_size, file);

    if emit_progress {
        telemetry.progress(&format!(
            "Starting merge into {} ({} segments)",
            output_path.display(),
            segments.len()
        ));
    }

    if segments.is_empty() {
        writer
            .flush()
            .with_context(|| format!("failed to flush output file: {}", output_path.display()))?;
        return Ok(MergeStats {
            segments: 0,
            unique_lines: 0,
            total_bytes: 0,
        });
    }

    let reader_buffer = reader_buffer_size(buffer_bytes, segments.len());

    let mut readers = Vec::with_capacity(segments.len());
    for segment in segments {
        readers.push(SegmentReader::open(&segment.path, reader_buffer)?);
    }

    let mut heap: BinaryHeap<Reverse<HeapEntry>> = BinaryHeap::with_capacity(segments.len());
    for (index, reader) in readers.iter_mut().enumerate() {
        if let Some(line) = reader.read_line(Vec::new())? {
            heap.push(Reverse(HeapEntry {
                segment_index: index,
                line,
            }));
        }
    }

    let mut last_line = Vec::new();
    let mut has_last = false;
    let mut total_bytes = 0u64;
    let mut unique_lines = 0u64;
    let mut next_progress = MERGE_PROGRESS_LINE_INTERVAL;

    while let Some(Reverse(entry)) = heap.pop() {
        let segment_index = entry.segment_index;
        let mut line = entry.line;
        let is_duplicate = has_last && last_line == line;
        if !is_duplicate {
            let line_len = line.len();
            line.push(b'\n');
            writer.write_all(&line).with_context(|| {
                format!("failed to write output data: {}", output_path.display())
            })?;
            line.pop();
            total_bytes += (line_len + 1) as u64;
            unique_lines += 1;
            if emit_progress && unique_lines >= next_progress {
                telemetry.progress(&format!(
                    "Merged {unique_lines} unique lines into {}",
                    output_path.display()
                ));
                next_progress = next_progress.saturating_add(MERGE_PROGRESS_LINE_INTERVAL);
            }

            std::mem::swap(&mut last_line, &mut line);
            has_last = true;
        }

        if let Some(next_line) = readers[segment_index].read_line(line)? {
            heap.push(Reverse(HeapEntry {
                segment_index,
                line: next_line,
            }));
        }
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush output file: {}", output_path.display()))?;

    if emit_progress && unique_lines > 0 && unique_lines < MERGE_PROGRESS_LINE_INTERVAL {
        telemetry.progress(&format!(
            "Merged {unique_lines} unique lines into {}",
            output_path.display()
        ));
    }

    Ok(MergeStats {
        segments: segments.len(),
        unique_lines,
        total_bytes,
    })
}

/// Determine where to place intermediate merge products, preferring the
/// segment directory when available.
fn determine_scratch_dir(segments: &[Segment], output_path: &Path) -> PathBuf {
    segments
        .first()
        .and_then(|segment| segment.path.parent().map(|p| p.to_path_buf()))
        .or_else(|| output_path.parent().map(|p| p.to_path_buf()))
        .unwrap_or_else(|| PathBuf::from("."))
}

/// Compute the per-segment reader buffer size bounded by global limits.
fn reader_buffer_size(buffer_bytes: usize, segment_count: usize) -> usize {
    if segment_count == 0 {
        return MERGE_READER_BUFFER_MIN;
    }
    let per_reader = buffer_bytes / segment_count;
    let base = per_reader.max(MERGE_READER_BUFFER_MIN);
    base.min(MERGE_READER_BUFFER_CAP)
}

/// Determine the maximum number of files that can be merged concurrently,
/// reserving headroom for other file descriptors.
fn max_merge_fan_in() -> usize {
    let fd_limit = env_max_open_files()
        .or_else(system_fd_limit)
        .unwrap_or(DEFAULT_MAX_OPEN_FILES);
    // Leave headroom for other descriptors and the output writer handle.
    let available = fd_limit.saturating_sub(FD_RESERVE);
    let fan_in = available.saturating_sub(1);
    fan_in.clamp(MIN_MERGE_FAN_IN, MAX_MERGE_FAN_IN)
}

/// Inspect the `DEDUX_MAX_OPEN_FILES` override when present.
fn env_max_open_files() -> Option<usize> {
    let raw = env::var("DEDUX_MAX_OPEN_FILES").ok()?;
    raw.trim().parse().ok()
}

#[cfg(unix)]
/// Query the operating system for the current soft file-descriptor limit.
fn system_fd_limit() -> Option<usize> {
    unsafe {
        let mut limit = std::mem::zeroed::<libc::rlimit>();
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) == 0 {
            if limit.rlim_cur != libc::RLIM_INFINITY {
                return Some(limit.rlim_cur as usize);
            }
        }
        let val = libc::sysconf(libc::_SC_OPEN_MAX);
        if val > 0 { Some(val as usize) } else { None }
    }
}

#[cfg(windows)]
/// Windows lacks a simple RLIMIT analogue; fall back to defaults.
fn system_fd_limit() -> Option<usize> {
    // Windows does not expose a simple RLIMIT analogue; use None to fall back to defaults.
    None
}

#[cfg(not(any(unix, windows)))]
/// No platform-specific descriptor limit is available; fall back to defaults.
fn system_fd_limit() -> Option<usize> {
    None
}

/// Buffered reader wrapper that tracks its own line buffer to minimise
/// allocations during merge.
struct SegmentReader {
    reader: BufReader<File>,
}

impl SegmentReader {
    /// Open a segment for reading with the provided buffer capacity.
    fn open(path: &Path, buffer_bytes: usize) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open segment file: {}", path.display()))?;
        let buffer = BufReader::with_capacity(buffer_bytes, file);
        Ok(Self { reader: buffer })
    }

    /// Read the next line from the segment, returning `None` on EOF.
    fn read_line(&mut self, mut buffer: Vec<u8>) -> Result<Option<Vec<u8>>> {
        buffer.clear();
        let bytes = self.reader.read_until(b'\n', &mut buffer)?;
        if bytes == 0 {
            return Ok(None);
        }
        if buffer.last() == Some(&b'\n') {
            buffer.pop();
        }
        Ok(Some(buffer))
    }
}

/// Entry stored in the min-heap during multi-way merging.
#[derive(Debug)]
struct HeapEntry {
    segment_index: usize,
    line: Vec<u8>,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.line == other.line
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.line.cmp(&other.line))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.line.cmp(&other.line)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::Telemetry;
    use std::fs;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_dir() -> PathBuf {
        let base = env::temp_dir();
        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        base.join(format!("dedux_test_{pid}_{ts}"))
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
}
