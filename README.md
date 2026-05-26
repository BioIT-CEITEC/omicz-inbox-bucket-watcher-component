# S3 Bucket Watcher Service

A stateless Python service that monitors S3 buckets for new directories containing SHA-256 checksum manifests, validates file integrity, and transfers valid data to a storage bucket.

## Features

- **Polling-based discovery** of new directories with configurable prefixes
- **SHA-256 validation** of all files against checksum manifests
- **Conditional transfer** with all-or-nothing semantics per directory
- **Stateless design** with no external database dependencies
- **Fixed-interval processing** with simple, predictable behavior

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Inbox Bucket   │────▶│  Watcher Service │────▶│ Storage Bucket  │
│  (source data)  │     │  (this service)  │     │ (validated data)│
└─────────────────┘     └──────────────────┘     └─────────────────┘
                               │
                               ▼
                        ┌─────────────────┐
                        │ Checksum Bucket │
                        │ (success/failure│
                        │     markers)    │
                        └─────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager
- AWS credentials or S3-compatible storage credentials

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd omicz-inbox-scanner

# Install dependencies using uv
uv sync

# Run tests
uv run pytest tests/ -v
```

### Configuration

Copy the example configuration file and customize it:

```bash
cp config.yaml.example config.yaml
```

Edit `config.yaml` with your settings:

```yaml
s3:
  inbox_bucket: "my-org-inbox"
  storage_bucket: "my-org-storage"
  checksum_bucket: "my-org-checksums"
  region: "eu-east-1"
  endpoint_url: "https://s3.example.com"

watcher:
  watched_prefixes:
    - "uploads/project-a/"
    - "uploads/project-b/"
  poll_interval_seconds: 60
  settling_period_seconds: 30
  max_concurrent_directories: 5

transfer:
  max_concurrent_copies: 10
  verify_after_copy: true

log_level: "INFO"
```

### Environment Variables

All configuration options can be overridden via environment variables:

```bash
# S3 Configuration
export S3WATCHER_S3_INBOX_BUCKET="my-inbox-bucket"
export S3WATCHER_S3_STORAGE_BUCKET="my-storage-bucket"
export S3WATCHER_S3_CHECKSUM_BUCKET="my-checksum-bucket"
export S3WATCHER_S3_REGION="eu-east-1"
export S3WATCHER_S3_ENDPOINT_URL="http://localhost:9000"  # For MinIO

# Watcher Configuration
export S3WATCHER_WATCHER_WATCHED_PREFIXES='["uploads/"]'
export S3WATCHER_WATCHER_POLL_INTERVAL_SECONDS=30
export S3WATCHER_WATCHER_SETTLING_PERIOD_SECONDS=10

# Logging
export S3WATCHER_LOG_LEVEL="DEBUG"
```

### Running the Service

```bash
# Run with configuration file
uv run python -m src --config config.yaml

# Run in single-scan mode (process once and exit)
uv run python -m src --config config.yaml --once

# Run with custom log level
uv run python -m src --config config.yaml --log-level DEBUG
```

## Usage

### How It Works

1. **Uploader** deposits data files and a `{sha256}.CHECKSUM` manifest file to the inbox bucket
2. **Watcher Service** polls the inbox bucket at configured intervals
3. **Directory Scanner** discovers directories containing `.CHECKSUM` files
4. **Checksum Validator** validates all files using set-based SHA-256 comparison
5. **Transfer Manager** copies valid files to the storage bucket (server-side copy)
6. **Cleanup Manager** removes processed files from the inbox bucket
7. **Checksum Reporter** writes success/failure markers to the checksum bucket

### Checksum File Format

The `.CHECKSUM` file contains only SHA-256 hashes (one per line), without filenames:

```
e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8
6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090
```

The filename of the CHECKSUM file is the SHA-256 hash of its content:

```
abc123def456...CHECKSUM  # where abc123def456... = sha256(file_content)
```

### Directory Structure

**Inbox Bucket:**
```
uploads/project-a/
  run-1/
    abc123def456...CHECKSUM
    data.bin
    metadata.json
```

**Storage Bucket (after successful transfer):**
```
uploads/project-a/
  run-1/
    data.bin
    metadata.json
```

**Checksum Bucket:**
```
abc123def456....CHECKSUM    # Empty file = success
abc123def456....CHECKSUM    # Non-empty = failure (contains original hashes of failed files)
```

## Docker

Build and run using Docker:

```bash
# Build the image
docker build -t s3-watcher .

# Run the container
docker run -d \
  -v $(pwd)/config.yaml:/app/config.yaml \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  s3-watcher
```

## Development

### Running Tests

```bash
# Run all tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ -v --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_validator.py -v
```

### Code Structure

```
src/s3_watcher/
├── __init__.py          # Package initialization
├── __main__.py          # Entry point for python -m
├── cli.py               # Click CLI
├── config.py            # Pydantic Settings configuration
├── s3_client.py         # Async S3 abstraction with aiobotocore
├── scanner.py           # DirectoryScanner - discovers directories
├── validator.py         # ChecksumValidator - set-based SHA-256 validation
├── transfer.py          # TransferManager - copies data to storage
├── cleanup.py           # CleanupManager - batch-deletes from inbox
├── reporter.py          # ChecksumReporter - writes markers
├── cache.py             # ProcessingCache - ephemeral tracking
├── orchestrator.py      # Orchestrator - coordinates lifecycle
├── scheduler.py         # Scheduler - fixed-interval polling
└── models.py            # Data models
```

## IAM Permissions

The service requires the following IAM permissions:

**Inbox Bucket:**
```json
{
  "Effect": "Allow",
  "Action": ["s3:ListBucket", "s3:GetObject", "s3:DeleteObject"],
  "Resource": ["arn:aws:s3:::my-org-inbox", "arn:aws:s3:::my-org-inbox/*"]
}
```

**Storage Bucket:**
```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
  "Resource": ["arn:aws:s3:::my-org-storage", "arn:aws:s3:::my-org-storage/*"]
}
```

**Checksum Bucket:**
```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject", "s3:GetObject"],
  "Resource": "arn:aws:s3:::my-org-checksums/*"
}
```

## License

MIT License
