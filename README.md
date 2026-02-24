# NCBI Async Downloader

High-performance asynchronous downloader for NCBI datasets using HTTP/2 and in-memory streaming.

## Why?
Standard tools (wget/curl) are slow due to single connection limits. 
Aspera Connect is proprietary and often blocked by firewalls.
This tool maximizes bandwidth using Python `asyncio`.

## Features
- 🚀 **Fast**: Multi-connection downloading using `httpx` & `asyncio`.
- 🛡️ **Reliable**: Auto-retries, atomic writes (`os.pwrite`), resume capability.
- 🌊 **Streaming**: Processing data in-memory without disk I/O (optional).
- 📊 **UI**: Rich console output with real-time progress.
- 🧬 **Bio-ready**: Checksum validation (MD5) and automatic unpacking.

## Usage
```bash
python main.py https://ftp.ncbi.../file.gz --threads 10