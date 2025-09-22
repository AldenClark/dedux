//! Build sorted, deduplicated segment runs from the raw input stream.

use std::fs::{self, File};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread;

use ahash::RandomState;
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{Receiver, Sender, bounded};
use hashbrown::HashTable;

use crate::config::PipelineConfig;
use crate::io_reader::FileLineReader;
use crate::telemetry::{Telemetry, format_bytes};

const ENTRY_OVERHEAD_BYTES: usize = 48;
const MIN_LINE_SIZE_HINT: usize = 16;
const BATCH_CHANNEL_CAPACITY: usize = 256;
const LINE_PROGRESS_INTERVAL: u64 = 1_000_000;
const SEGMENT_PROGRESS_INTERVAL: usize = 25;
const SHARD_BATCH_TARGET_LINES: usize = 256;
const SHARD_BATCH_TARGET_BYTES: usize = 256 * 1024;

/// Metadata describing an on-disk sorted segment of unique lines.
#[derive(Debug, Clone)]
pub struct Segment {
    pub path: PathBuf,
    pub bytes: u64,
}

/// Aggregate statistics collected during segment construction.
#[derive(Debug, Clone)]
pub struct ChunkBuildStats {
    pub segments: usize,
    pub total_input_lines: u64,
    pub unique_lines: u64,
    pub bytes_written: u64,
}

/// Summary produced by each worker thread when it finishes processing.
struct WorkerStats {
    segments: Vec<Segment>,
    unique_lines: u64,
    bytes_written: u64,
}

/// Stream the input, dispatch work to shard workers, and materialize sorted
/// segment files containing unique lines.
pub fn build_segments(
    config: &PipelineConfig,
    segments_dir: &Path,
    telemetry: &Telemetry,
) -> Result<(Vec<Segment>, ChunkBuildStats)> {
    fs::create_dir_all(segments_dir).with_context(|| {
        format!(
            "failed to create segment directory: {}",
            segments_dir.display()
        )
    })?;

    let worker_threads = config.worker_threads.max(1);
    let per_worker_limit = if config.chunk_memory_bytes == 0 {
        0
    } else {
        (config.chunk_memory_bytes / worker_threads).max(ENTRY_OVERHEAD_BYTES + MIN_LINE_SIZE_HINT)
    };

    let mut reader =
        FileLineReader::open(&config.input, config.read_buffer_bytes, config.trim_crlf)?;
    let mut total_input_lines = 0u64;
    let mut total_unique_lines = 0u64;
    let mut total_bytes_written = 0u64;
    let mut all_segments = Vec::new();
    let mut next_line_progress = LINE_PROGRESS_INTERVAL;

    let segment_counter = Arc::new(AtomicUsize::new(0));
    let shard_hasher = RandomState::new();

    thread::scope(|scope| -> Result<()> {
        let mut senders: Vec<Sender<Vec<(u64, Box<[u8]>)>>> = Vec::with_capacity(worker_threads);
        let mut handles = Vec::with_capacity(worker_threads);

        for _ in 0..worker_threads {
            let (tx, rx) = bounded::<Vec<(u64, Box<[u8]>)>>(BATCH_CHANNEL_CAPACITY);
            senders.push(tx);
            let segments_dir = segments_dir.to_path_buf();
            let segment_counter = Arc::clone(&segment_counter);
            let worker_telemetry = *telemetry;
            handles.push(scope.spawn(move || {
                worker_loop(
                    rx,
                    segments_dir,
                    segment_counter,
                    per_worker_limit,
                    config.segment_buffer_bytes,
                    worker_telemetry,
                )
            }));
        }

        let mut shard_batches: Vec<Vec<(u64, Box<[u8]>)>> = (0..worker_threads)
            .map(|_| Vec::with_capacity(SHARD_BATCH_TARGET_LINES))
            .collect();
        let mut shard_batch_bytes = vec![0usize; worker_threads];

        reader.consume_lines(|line| -> Result<()> {
            if config.skip_empty && line.is_empty() {
                return Ok(());
            }
            total_input_lines += 1;
            if total_input_lines >= next_line_progress {
                telemetry.progress(&format!("Processed {total_input_lines} input lines so far"));
                next_line_progress = next_line_progress.saturating_add(LINE_PROGRESS_INTERVAL);
            }
            let mut hasher = shard_hasher.build_hasher();
            line.hash(&mut hasher);
            let hash = hasher.finish();
            let shard_index = if worker_threads == 1 {
                0
            } else {
                (hash as usize) % worker_threads
            };
            let boxed: Box<[u8]> = line.to_owned().into_boxed_slice();
            let batch = &mut shard_batches[shard_index];
            batch.push((hash, boxed));
            let batch_bytes = &mut shard_batch_bytes[shard_index];
            *batch_bytes = batch_bytes.saturating_add(line.len());

            if batch.len() >= SHARD_BATCH_TARGET_LINES || *batch_bytes >= SHARD_BATCH_TARGET_BYTES {
                let mut to_send = Vec::with_capacity(batch.len());
                to_send.extend(batch.drain(..));
                *batch_bytes = 0;
                senders
                    .get(shard_index)
                    .expect("shard index out of range")
                    .send(to_send)
                    .map_err(|_| anyhow!("worker thread {shard_index} terminated prematurely"))?;
            }
            Ok(())
        })?;

        for (shard_index, (batch, bytes)) in shard_batches
            .iter_mut()
            .zip(shard_batch_bytes.iter_mut())
            .enumerate()
        {
            if !batch.is_empty() {
                let mut to_send = Vec::with_capacity(batch.len());
                to_send.extend(batch.drain(..));
                *bytes = 0;
                senders
                    .get(shard_index)
                    .expect("shard index out of range")
                    .send(to_send)
                    .map_err(|_| anyhow!("worker thread {shard_index} terminated prematurely"))?;
            }
        }

        drop(senders);

        for handle in handles {
            let worker_stats = handle.join().expect("dedupe worker thread panicked")?;
            total_unique_lines += worker_stats.unique_lines;
            total_bytes_written += worker_stats.bytes_written;
            all_segments.extend(worker_stats.segments);
        }

        Ok(())
    })?;

    all_segments.sort_by(|a, b| a.path.cmp(&b.path));

    let stats = ChunkBuildStats {
        segments: all_segments.len(),
        total_input_lines,
        unique_lines: total_unique_lines,
        bytes_written: total_bytes_written,
    };
    Ok((all_segments, stats))
}

/// Consume lines for a single shard, flushing to disk when memory thresholds
/// are reached.
fn worker_loop(
    receiver: Receiver<Vec<(u64, Box<[u8]>)>>,
    segments_dir: PathBuf,
    segment_counter: Arc<AtomicUsize>,
    chunk_memory_limit: usize,
    segment_buffer_bytes: usize,
    telemetry: Telemetry,
) -> Result<WorkerStats> {
    let mut chunk = ChunkState::new(chunk_memory_limit);
    let mut segments = Vec::new();
    let mut unique_lines = 0u64;
    let mut bytes_written = 0u64;

    while let Ok(batch) = receiver.recv() {
        for (hash, line) in batch {
            if chunk.insert_boxed(hash, line) {
                unique_lines += 1;
            }
            if chunk.should_flush() {
                let segment_id = segment_counter.fetch_add(1, Ordering::Relaxed);
                if let Some(segment) =
                    flush_chunk(&mut chunk, segment_id, &segments_dir, segment_buffer_bytes)?
                {
                    bytes_written += segment.bytes;
                    let latest_bytes = segment.bytes;
                    segments.push(segment);
                    log_segment_progress(&telemetry, segment_id + 1, latest_bytes);
                }
            }
        }
    }

    if chunk.has_data() {
        let segment_id = segment_counter.fetch_add(1, Ordering::Relaxed);
        if let Some(segment) =
            flush_chunk(&mut chunk, segment_id, &segments_dir, segment_buffer_bytes)?
        {
            bytes_written += segment.bytes;
            let latest_bytes = segment.bytes;
            segments.push(segment);
            log_segment_progress(&telemetry, segment_id + 1, latest_bytes);
        }
    }

    Ok(WorkerStats {
        segments,
        unique_lines,
        bytes_written,
    })
}

/// Emit periodic progress updates while segment files are generated.
fn log_segment_progress(telemetry: &Telemetry, segment_number: usize, segment_bytes: u64) {
    if segment_number == 1 || segment_number % SEGMENT_PROGRESS_INTERVAL == 0 {
        telemetry.progress(&format!(
            "Generated {segment_number} segments so far (latest ~{} written)",
            format_bytes(segment_bytes)
        ));
    }
}

/// Persist the current chunk to disk as a sorted segment file.
fn flush_chunk(
    chunk: &mut ChunkState,
    segment_id: usize,
    segments_dir: &Path,
    buffer_bytes: usize,
) -> Result<Option<Segment>> {
    let values = chunk.drain_sorted();
    if values.is_empty() {
        return Ok(None);
    }

    let path = segments_dir.join(format!("segment_{:08}.run", segment_id));
    let file = File::create(&path)
        .with_context(|| format!("failed to create segment file: {}", path.display()))?;
    let buffer_size = buffer_bytes.max(64 * 1024);
    let mut writer = BufWriter::with_capacity(buffer_size, file);
    let mut bytes_written = 0u64;

    for value in values {
        writer
            .write_all(&value)
            .with_context(|| format!("failed to write segment data: {}", path.display()))?;
        writer
            .write_all(b"\n")
            .with_context(|| format!("failed to write newline for segment: {}", path.display()))?;
        bytes_written += (value.len() + 1) as u64;
    }

    writer
        .flush()
        .with_context(|| format!("failed to flush segment file: {}", path.display()))?;

    Ok(Some(Segment {
        path,
        bytes: bytes_written,
    }))
}

/// Tracks the dedupe state for a worker shard using a hash table plus rough
/// memory accounting.
struct ChunkState {
    table: HashTable<HashedLine>,
    unique_bytes: usize,
    limit_bytes: usize,
    limit_entries: usize,
}

struct HashedLine {
    hash: u64,
    bytes: Box<[u8]>,
}

impl HashedLine {
    fn new(hash: u64, bytes: Box<[u8]>) -> Self {
        Self { hash, bytes }
    }

    fn into_bytes(self) -> Box<[u8]> {
        self.bytes
    }
}

impl PartialEq for HashedLine {
    fn eq(&self, other: &Self) -> bool {
        self.bytes.as_ref() == other.bytes.as_ref()
    }
}

impl Eq for HashedLine {}

impl Hash for HashedLine {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl ChunkState {
    /// Create a new chunk enforcing the provided memory bound, or unlimited if
    /// the bound is zero.
    fn new(limit_bytes: usize) -> Self {
        let (limit_bytes, limit_entries) = if limit_bytes == 0 {
            (usize::MAX, usize::MAX)
        } else {
            let approx_entry = ENTRY_OVERHEAD_BYTES + MIN_LINE_SIZE_HINT;
            let entries = (limit_bytes / approx_entry).max(1);
            (limit_bytes, entries)
        };
        Self {
            table: HashTable::new(),
            unique_bytes: 0,
            limit_bytes,
            limit_entries,
        }
    }

    /// Track a line in the chunk, returning whether it was new data.
    fn insert_boxed(&mut self, hash: u64, line: Box<[u8]>) -> bool {
        let len = line.len();
        let line_slice = line.as_ref();
        if self
            .table
            .find(hash, |existing| existing.bytes.as_ref() == line_slice)
            .is_some()
        {
            return false;
        }

        let hashed_line = HashedLine::new(hash, line);
        self.table
            .insert_unique(hash, hashed_line, |entry| entry.hash);
        self.unique_bytes = self.unique_bytes.saturating_add(len + ENTRY_OVERHEAD_BYTES);
        true
    }

    /// Check whether the chunk exceeded either its byte or entry thresholds.
    fn should_flush(&self) -> bool {
        if self.table.is_empty() {
            return false;
        }
        let bytes_full = self.limit_bytes != usize::MAX && self.unique_bytes >= self.limit_bytes;
        let entries_full =
            self.limit_entries != usize::MAX && self.table.len() >= self.limit_entries;
        bytes_full || entries_full
    }

    /// Determine whether the chunk still contains data.
    fn has_data(&self) -> bool {
        !self.table.is_empty()
    }

    /// Drain all values from the chunk in lexically sorted order.
    fn drain_sorted(&mut self) -> Vec<Box<[u8]>> {
        let mut values: Vec<Box<[u8]>> = self.table.drain().map(HashedLine::into_bytes).collect();
        values.sort_unstable_by(|a, b| a.as_ref().cmp(b.as_ref()));
        self.unique_bytes = 0;
        values
    }
}
