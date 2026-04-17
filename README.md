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

HydraStream is a concurrent HTTP downloader written in Python. It fetches file chunks concurrently using HTTP Range requests and utilizes an internal min-heap to reorder out-of-sequence chunks in memory. This enables the piping of large remote files directly to `stdout` without requiring intermediate disk storage.

## Core Characteristics

* **Chaos-Tested Resilience & "Laptop-Lid" Recovery**: Hardened against severe network and OS anomalies using continuous CI fault injection (`tc qdisc` and `iptables`). HydraStream guarantees deterministic recovery from total internet outages, massive packet loss, and OS-level process suspensions (e.g., closing your laptop lid). If a socket dies, the affected worker seamlessly requeues the chunk and resumes via HTTP `Range` requests without losing a single verified byte.
* **In-Memory Assembly & On-the-Fly Hashing**: Converts concurrent, out-of-order HTTP chunks into a sequential byte stream using a min-heap. Enables direct piping to `stdout`. Computes cryptographic hashes (MD5, SHA-256, BLAKE2, etc.) incrementally as the stream is yielded, ensuring integrity without buffering the full file.
* **POSIX-Compliant Telemetry**: Strictly adheres to the Unix philosophy by routing all diagnostic outputs, progress bars, and warnings to `stderr`. This guarantees a completely pure `stdout` for binary data pipes. Supports structured JSON Lines logging for CI/CD integration.
* **Network Resilience & Chaos-Tested Reliability**: Hardened against adversarial network conditions using Property-Based Testing (Hypothesis) and fault injection. Guarantees deadlock-free recovery, pipeline termination, and data integrity during `429`/`503` cascades, connection drops, and missing `Range` headers.
* **Strict Data Verification**: Enforces multi-layered integrity checks. Automatically validates payload size against remote metadata and performs strict post-download checksum validation to eliminate silent data corruption.
* **Auto-Scaling Concurrency & Throttling**: Implements an AIMD (Additive Increase/Multiplicative Decrease) algorithm to dynamically adjust active workers based on network health. Supports hard bandwidth throttling (`--limit`) for controlled environment execution.
* **Actor-Based Architecture & Lock-Free Synchronization**: Pipeline components (feeders, resolvers, dispatchers, workers) operate as isolated asynchronous actors (CSP pattern). Solves Fan-In/Fan-Out race conditions using prioritized sentinel values (poison pills) instead of shared-memory mutexes.
* **Zero-Lock Disk I/O**: Leverages `os.pwrite` within a dedicated thread pool to write scattered chunks concurrently. Completely bypasses GIL contention and traditional file locking mechanisms during disk operations.
* **Dry-Run Protocol**: Provides a safe simulation mode (`--dry-run`) to preemptively fetch remote metadata, verify available local disk space, and resolve target hashes without allocating space or initiating data transfer.
* **TLS Fingerprint Spoofing**: Integrates `curl_cffi` to mimic real browser TLS signatures (e.g., Chrome 120), bypassing strict WAFs (Web Application Firewalls) and Deep Packet Inspection (DPI) heuristics.
* **Layered Configuration & Domain-Driven Design**: Features a strict boundary between the core engine and CLI. Seamlessly merges CLI arguments and global TOML configurations (`~/.config/hydrastream/config.toml`) via a late-binding validation layer.


## Installation

Requires Python 3.11+.

```bash
uv tool install hydrastream
```
or
```bash
pipx install hydrastream
```

## Usage

### 1. Download to Disk
Downloads the specified file to the output directory using dynamically scaled threads.:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="HydraStream Demo" width="800">
</p>

### 2. Stream to stdout (Pipe)
Downloads the file in memory and streams binary data to `stdout`. The `--quiet` (`-q`) flag is used to suppress logging output to `stderr`.:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream -q | zcat | wc -l
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Pipeline-Streaming-Demo.gif" alt="Pipeline Streaming Demo" width="800">
</p>

### 3. Batch Processing
Reads target URLs from a local file.

```bash
hs --input urls.txt --threads 20 --output ./datasets
```

## Configuration

HydraStream supports layered configuration. Default parameters can be defined in a TOML file located at `~/.config/hydrastream/config.toml`. CLI arguments override these defaults.

```toml
# ~/.config/hydrastream/config.toml
threads = 128
output_dir = "~/downloads"
verify = true
speed_limit = 50.0
min-chunk-mb = 5
```

### 4. Python API

```python
import asyncio
from hydrastream import HydraClient, HydraConfig

async def main():
    config = HydraConfig(threads=20, quiet=True)
    urls = ["https://example.com/file1.gz"]

    async with HydraClient(config=config) as client:
        # Returns an async generator yielding (filename, chunk_generator)
        async for filename, file_stream in await client.stream(urls):
            async for chunk in file_stream:
                sys.stdout.buffer.write(chunk)

if __name__ == "__main__":
    asyncio.run(main())
```

## CLI Options

HydraStream supports layered configuration. Options can be passed as CLI arguments or defined in `~/.config/hydrastream/config.toml`. CLI flags take precedence.

| Option | Shortcut | Default | Description |
| :--- | :---: | :---: | :--- |
| `LINKS` | - | `None` | One or multiple target URLs to download (positional argument). |
| `--input` | `-i` | `None` | Read URLs from a text file or `-` for stdin. |
| `--typehash` | `-th` | `md5` | Hash algorithm type (e.g., `md5`, `sha256`). |
| `--hash` | - | `None` | Expected hash checksum (applicable only for a single URL). |
| `--output` | `-o` | `download/` | Destination directory for downloaded files. |
| `--threads` | `-t` | `Auto` | Number of concurrent download connections (scales up to 128). |
| `--stream` | `-s` | `False` | Enable streaming mode (redirects binary data to `stdout`). |
| `--dry-run` | - | `False` | Simulate the process (fetch metadata, check disk space) without downloading. |
| `--min-chunk-mb` | - | `1` | Minimum chunk size in Megabytes for standard disk downloads. |
| `--stream-chunk-mb` | - | `5` | Target chunk size in Megabytes for streaming mode. |
| `--buffer` | `-b` | `None` | Maximum stream buffer size in Megabytes to prevent OOM. |
| `--limit` | `-l` | `None` | Global download bandwidth throttle limit in MB/s. |
| `--no-ui` | `-nu` | `False` | Disable GUI (progress bars). Leaves plain text logs. |
| `--quiet` | `-q` | `False` | Dead silence. No console output at all. Logs are still written to file. |
| `--json` | `-j` | `False` | Output logs in structured JSON Lines format. |
| `--verify` / `--no-verify` | `-V` / `-N` | `True` | Verify the downloaded file hash. Use `--no-verify` to skip. |
| `--browser` | `-B` | `chrome120` | Browser TLS fingerprint to impersonate (e.g., `chrome120`, `safari153`). |
| `--debug` | `-d` | `False` | Enable debug mode (propagates full exception tracebacks). |
| `--version` | `-v` | - | Show application version and exit. |

## Roadmap

### **v2.0: Rust Core:**

Port the core engine to Rust (`tokio`/`reqwest`) with a `PyO3` wrapper to bypass the Python GIL and improve multi-core execution.

## License

MIT License. See the [LICENSE](LICENSE) file for details.
