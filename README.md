# HydraStream

[![PyPI version](https://badge.fury.io/py/hydrastream.svg)](https://pypi.org/project/hydrastream/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage: 90%](https://img.shields.io/badge/coverage-90%25-brightgreen.svg)](https://github.com/Zhukovetski/HydraStream)
[![Tests](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/HydraStream/HydraStream)

<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Demo.gif" alt="HydraStream Demo" width="800">
</p>

HydraStream is a concurrent HTTP downloader written in Python. It supports multipart downloading and in-memory chunk reordering, allowing you to stream remote files directly to `stdout` without writing to disk.

## Motivation

Standard tools like `wget` or `curl` stream sequentially but are limited to a single connection. Tools like `aria2` download concurrently but require disk I/O to reassemble the file.

This project bridges the gap: it fetches chunks concurrently via `httpx` and `uvloop`, buffers them in memory using a min-heap, and yields a sequential byte stream. This is useful for piping large remote files (e.g., genomics data, DB dumps) directly into Unix tools (`zcat`, `grep`, `tar`) when local disk space is constrained.

## Features

* **Concurrent Downloading**: Uses HTTP Range requests to fetch parts simultaneously.
* **Stream Reordering**: Converts out-of-order chunks into a sequential stream via an internal priority queue.
* **Rate Limiting & Backoff**: AIMD-based rate limiter to handle `429 Too Many Requests` and exponential backoff for network drops.
* **Resumption**: Saves partial state for disk-mode downloads to resume after interruptions.
* **POSIX Compliance**: In stream mode or `--quiet` mode, logs are routed to `stderr` and data to `stdout`.

## Benchmarks
Tested on Ubuntu (GitHub Actions) downloading a 1GB `.fna.gz` dataset from NCBI.
Despite the CPU overhead of pure Python, HydraStream outperforms optimized C/C++ binaries in wall-clock time by effectively multiplexing connections and utilizing lock-free I/O (`os.pwrite`).

| Tool | Connections | Real Time (Wall-clock) | User Time (CPU) | Sys Time (Kernel) |
| :--- | :---: | :---: | :---: | :---: |
| **HydraStream** | 20 | **9.480s** | 8.430s | 1.946s |
| **wget** | 1 | 10.464s | 0.853s | 1.764s |
| **aria2c** | 10 | 11.081s | 0.986s | 1.850s |

## Installation

Requires Python 3.11+.

```bash
uv tool install git+https://github.com/Zhukovetski/HydraStream.git
```
or
```bash
pipx install git+https://github.com/Zhukovetski/HydraStream.git
```

## Usage

### 1. Download to Disk
Download a file using 20 connections:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="HydraStream Demo" width="800">
</p>

### 2. Stream to stdout (Pipe)
Download concurrently and pipe directly into a decompressor:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream -q | zcat | wc -l
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Pipeline-Streaming-Demo.gif" alt="Pipeline Streaming Demo" width="800">
</p>


### 3. Python API
```python
import asyncio
from hydrastream import HydraClient

async def main():
    urls =["https://example.com/file1.gz"]
    async with HydraClient(threads=10, quiet=True) as client:
        async for filename, stream in client.stream(urls):
            async for chunk in stream:
                pass # Process chunk bytes

if __name__ == "__main__":
    asyncio.run(main())
```

## CLI Options

| Option | Shortcut | Default | Description |
| :--- | :---: | :---: | :--- |
| `URLS` | - | Required | One or multiple URLs to download. |
| `--threads` | `-t` | `1` | Number of concurrent connections. |
| `--output` | `-o` | `download/` | Output directory. |
| `--stream` | `-s` | `False` | Enable streaming mode (redirects data to `stdout`). |
| `--no-ui` | `-nu` | `False` | Disables progress bars, leaves plain text logs. |
| `--quiet` | `-q` | `False` | Silence console output. Logs are still written to file. |
| `--md5` | | `None` | Expected MD5 hash (single URL only). |
| `--buffer` | `-b` | `threads * 10MB` | Maximum stream buffer size in bytes. |

## Roadmap

### Technical Roadmap v1.2 (The Resilience Update)

#### 1. Network Layer Overhaul
*   **Objective:** Bypass Deep Packet Inspection (WAF / TLS Fingerprinting).
*   **Action:** Replace `httpx` with `curl_cffi.requests.AsyncSession`.
*   **Implementation:**
    *   Integrate the `impersonate="chrome110"` (or equivalent) parameter to spoof TLS fingerprints.
    *   Adapt existing interfaces (timeouts, connection pools, error handling) to the `curl_cffi` API.
    *   Remove manual header spoofing (`User-Agent`, `Accept-Encoding`), as `curl_cffi` handles this natively at the C-library level.

#### 2. Input/Output Flexibility
*   **Objective:** Support batch processing and Unix pipeline integration.
*   **Action 1:** Read URL lists from a file or standard input.
    *   Implement the `-i / --input` flag (supporting `stdin` via the `-` character).
*   **Action 2:** Dry-Run mode.
    *   Add the `--dry-run` flag.
    *   Terminate the pipeline immediately after the `TaskProducer` execution.
    *   Output a summary table (file count, total size, MD5 resolution status) without allocating disk space or initializing the `Dispatcher`.

#### 3. Graceful Degradation
*   **Objective:** Prevent data corruption and bandwidth waste on servers lacking `Range` request support.
*   **Action:** Analyze the `Accept-Ranges` header during the `TaskProducer` HEAD request.
*   **Implementation:**
    *   If `Accept-Ranges: none` is detected, or the server ignores the `Range` header (returning the full file on a partial GET), force a fallback to single-threaded mode (1 chunk per file).
    *   Bypass `PriorityQueue` chunking logic for the affected file; the download proceeds linearly via a single worker.

#### 4. Architecture & Protocol Design
*   **Objective:** Decouple the storage subsystem to support future backends (e.g., S3, databases) and prepare for v2.0.
*   **Action:** Introduce a `StorageBackend` `Protocol` in `models.py` (or a dedicated `interfaces.py`).
*   **Implementation:**
    *   Refactor the current `StorageManager` into a `LocalStorageManager` that implements the protocol (requiring methods: `allocate_space`, `open`, `write_chunk`, `close`, `verify`).
    *   Ensure the core engine (`engine.py` and `Dispatcher`) depends exclusively on the `StorageBackend` interface, abstracting away the concrete implementation.

### **v1.3: Autonomous Worker Scaling:** 

Transition from a static thread pool to adaptive concurrency based on network conditions and downstream backpressure.

### **v2.0: Rust Core:**

Port the core engine to Rust (`tokio`/`reqwest`) with a `PyO3` wrapper to bypass the Python GIL and improve multi-core execution.

## License
MIT License.
