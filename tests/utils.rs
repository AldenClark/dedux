//! Test helpers for generating deterministic random data sets.

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

use rand::Rng;
use rand::distr::{Alphanumeric, Distribution, SampleString, Uniform};
use rand::prelude::IndexedRandom;
use rand::seq::SliceRandom;

pub const SHORT_LENGTH_CEILING: usize = 16;
const MAX_LENGTH: usize = 128;
const SHORT_FRACTION: f64 = 0.90;

/// Generate a collection of random password-like byte lines for tests.
///
/// The generator produces at least 90% of the lines with length less than or
/// equal to 16 bytes when the provided range allows. All emitted lines respect
/// the supplied `length_range`, capped at 128 bytes to ensure compatibility
/// with the deduplication pipeline's constraints.
#[allow(dead_code)]
pub fn generate_random_password_lines(count: usize, length_range: RangeInclusive<usize>) -> Vec<Vec<u8>> {
    let mut rng = rand::rng();
    generate_random_password_lines_with_rng(&mut rng, count, length_range)
}

/// Generate random password lines using the supplied RNG, enabling deterministic
/// sequences in tests.
pub fn generate_random_password_lines_with_rng<R: Rng + ?Sized>(
    rng: &mut R,
    count: usize,
    length_range: RangeInclusive<usize>,
) -> Vec<Vec<u8>> {
    if count == 0 {
        return Vec::new();
    }

    let (min_len, mut max_len) = (*length_range.start(), *length_range.end());
    assert!(min_len <= max_len, "invalid length range: start exceeds end");
    assert!(max_len > 0, "maximum length must be greater than zero");

    if max_len > MAX_LENGTH {
        max_len = MAX_LENGTH;
    }

    assert!(max_len >= min_len, "length range must include at least one value");

    let short_ceiling = SHORT_LENGTH_CEILING.min(max_len);
    let has_short = min_len <= short_ceiling;
    let has_long = max_len > short_ceiling;

    if !has_short {
        let distribution = Uniform::try_from(min_len..=max_len).expect("failed to construct uniform distribution for length range");
        return generate_lines(rng, count, &distribution, min_len, max_len);
    }

    let short_distribution = Uniform::try_from(min_len..=short_ceiling).expect("failed to construct short-length distribution");
    let mut lines = Vec::with_capacity(count);

    let short_goal = if has_long {
        ((count as f64 * SHORT_FRACTION).ceil() as usize).min(count)
    } else {
        count
    };
    let long_goal = count - short_goal;

    append_random_lines(rng, &mut lines, short_goal, &short_distribution, min_len, short_ceiling);

    if long_goal > 0 {
        let long_min = short_ceiling.saturating_add(1).max(min_len);
        let long_distribution = Uniform::try_from(long_min..=max_len).expect("failed to construct long-length distribution");
        append_random_lines(rng, &mut lines, long_goal, &long_distribution, long_min, max_len);
    }

    lines.shuffle(rng);
    lines
}

fn append_random_lines<R: Rng + ?Sized>(
    rng: &mut R,
    lines: &mut Vec<Vec<u8>>,
    count: usize,
    distribution: &Uniform<usize>,
    min_len: usize,
    max_len: usize,
) {
    if count == 0 {
        return;
    }

    for _ in 0..count {
        let len = distribution.sample(rng);
        debug_assert!(len >= min_len && len <= max_len);
        let line = Alphanumeric.sample_string(rng, len).into_bytes();
        lines.push(line);
    }
}

fn generate_lines<R: Rng + ?Sized>(
    rng: &mut R,
    count: usize,
    distribution: &Uniform<usize>,
    min_len: usize,
    max_len: usize,
) -> Vec<Vec<u8>> {
    let mut lines = Vec::with_capacity(count);
    for _ in 0..count {
        let len = distribution.sample(rng);
        debug_assert!(len >= min_len && len <= max_len);
        lines.push(Alphanumeric.sample_string(rng, len).into_bytes());
    }
    lines
}

/// Describe an on-disk dataset produced by [`generate_random_password_dataset_to_disk`].
#[allow(dead_code)]
pub struct RandomPasswordDataset {
    /// Location of the generated dataset.
    pub path: PathBuf,
    /// Unique values used to compose the dataset. Useful for assertions.
    pub unique_values: Vec<Vec<u8>>,
}

impl RandomPasswordDataset {
    /// Return the number of unique values used to build the dataset.
    #[allow(dead_code)]
    pub fn unique_line_count(&self) -> usize {
        self.unique_values.len()
    }

    /// Remove the underlying dataset file, ignoring errors if it was already deleted.
    #[allow(dead_code)]
    pub fn cleanup(&self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Generate a password-like dataset directly on disk, reusing a bounded pool of
/// unique lines to avoid excessive memory consumption.
#[allow(dead_code)]
pub fn generate_random_password_dataset_to_disk<R: Rng + ?Sized, P: AsRef<Path>>(
    rng: &mut R,
    directory: P,
    file_name: &str,
    total_lines: usize,
    unique_pool_size: usize,
    length_range: RangeInclusive<usize>,
) -> std::io::Result<RandomPasswordDataset> {
    let path = directory.as_ref().join(file_name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    if total_lines == 0 {
        File::create(&path)?;
        return Ok(RandomPasswordDataset {
            path,
            unique_values: Vec::new(),
        });
    }

    let bounded_unique = unique_pool_size.max(1).min(total_lines);
    let unique_values = generate_random_password_lines_with_rng(rng, bounded_unique, length_range.clone());

    let file = File::create(&path)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    for value in &unique_values {
        writer.write_all(value)?;
        writer.write_all(b"\n")?;
    }

    for _ in unique_values.len()..total_lines {
        let value = unique_values.choose(rng).expect("unique pool should not be empty");
        writer.write_all(value)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;

    Ok(RandomPasswordDataset { path, unique_values })
}
