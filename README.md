# Dedux

## English

### Overview

Dedux is a command-line pipeline for large-scale text deduplication. It reads arbitrarily large input files, shards the data across worker threads, builds sorted segment runs of unique lines, and merges them back into a single, deduplicated output file.

### Features

- Scales with available CPU cores while bounding memory per worker.
- Streaming reader that avoids loading entire files into memory.
- Sorted segment runs to keep merge fan-in manageable.
- Progress telemetry with per-stage and incremental updates.

### Quick Start

1. Install the latest stable Rust toolchain (`cargo`, `rustc`).
2. Build the binary: `cargo build --release`.
3. Run the deduper:
   ```bash
   cargo run --release -- <INPUT_FILE> --output <OUTPUT_DIR>
   ```
   The CLI writes its final output into `<OUTPUT_DIR>/<input>.deduped.*`. Use `--tmp-dir` to control where intermediate data lives.

### Configuration Highlights

- `--read-buf-mb` / `--segment-buf-mb` / `--output-buf-mb`: Tune buffered I/O sizes.
- `--chunk-mem-gb`: Cap per-worker in-memory dedupe size (0 = unlimited).
- `--threads`: Manually set worker count (0 = auto-detect).
- `--keep-empty` and `--no-trim-crlf`: Control input normalization.
- `--tmp-dir`: Provide a reusable location for temporary files if the default is unsuitable.

---

## 中文

### 概述

Dedux 是一个用于大规模文本去重的命令行流水线工具。它支持从超大输入文件中流式读取数据，按工作线程分片构建排好序的唯一行段，并在最后将这些段合并为一个已去重的输出文件。

### 特性

- 自动利用多核 CPU，同时限制每个工作线程的内存占用。
- 流式读取，避免一次性加载整份文件。
- 通过生成有序段文件控制最终合并时的文件数量。
- 逐阶段、逐进度的遥测输出，便于监控长时间运行。

### 快速开始

1. 安装最新稳定版 Rust 工具链（`cargo`、`rustc`）。
2. 构建可执行文件：`cargo build --release`。
3. 运行去重：
   ```bash
   cargo run --release -- <输入文件路径> --output <输出目录>
   ```
   程序会将最终结果写入 `<输出目录>/<输入文件名>.deduped.*`。若需自定义临时数据位置，可使用 `--tmp-dir` 参数。

### 配置要点

- `--read-buf-mb` / `--segment-buf-mb` / `--output-buf-mb`：调节各阶段的缓冲区大小。
- `--chunk-mem-gb`：限制每个工作线程的去重内存（0 表示不限）。
- `--threads`：手动指定线程数（0 表示自动探测）。
- `--keep-empty`、`--no-trim-crlf`：控制输入规范化行为。
- `--tmp-dir`：指定临时文件目录，避免使用默认随机目录。
