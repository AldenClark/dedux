//! Buffered line reader that streams data from disk while normalising line
//! endings.

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use anyhow::{Context, Result};
use memchr::memchr;

/// Streaming reader that yields lines without allocating new buffers for every
/// call and optionally trims CRLF endings.
pub struct FileLineReader {
    reader: BufReader<File>,
    carry: Vec<u8>,
    trim_crlf: bool,
}

impl FileLineReader {
    /// Open a file for buffered line-by-line reading.
    pub fn open(path: &Path, buffer_size: usize, trim_crlf: bool) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open file for reading: {}", path.display()))?;
        Ok(Self {
            reader: BufReader::with_capacity(buffer_size, file),
            carry: Vec::with_capacity(8192),
            trim_crlf,
        })
    }

    /// Consume the file, invoking `on_line` for each logical line.
    pub fn consume_lines<F>(&mut self, mut on_line: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        loop {
            let buffer = self.reader.fill_buf()?;
            if buffer.is_empty() {
                if !self.carry.is_empty() {
                    let len = trim_len(&self.carry, self.trim_crlf);
                    on_line(&self.carry[..len])?;
                    self.carry.clear();
                }
                break;
            }

            let mut start = 0usize;
            while let Some(pos) = memchr(b'\n', &buffer[start..]) {
                let end = start + pos;
                if self.carry.is_empty() {
                    let len = trim_len(&buffer[start..end], self.trim_crlf);
                    on_line(&buffer[start..start + len])?;
                } else {
                    self.carry.extend_from_slice(&buffer[start..end]);
                    let len = trim_len(&self.carry, self.trim_crlf);
                    on_line(&self.carry[..len])?;
                    self.carry.clear();
                }
                start = end + 1;
            }

            self.carry.extend_from_slice(&buffer[start..]);
            let consumed = buffer.len();
            self.reader.consume(consumed);
        }

        Ok(())
    }
}

/// Compute the logical line length, optionally trimming a trailing `\r`.
fn trim_len(bytes: &[u8], trim_crlf: bool) -> usize {
    if !trim_crlf || bytes.is_empty() {
        return bytes.len();
    }
    if bytes.ends_with(b"\r") {
        bytes.len().saturating_sub(1)
    } else {
        bytes.len()
    }
}
