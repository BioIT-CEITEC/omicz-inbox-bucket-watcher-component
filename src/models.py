"""Data models for S3 Bucket Watcher Service."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Set


@dataclass(frozen=True)
class S3Location:
    """Represents an S3 object location."""

    bucket: str
    key: str


@dataclass
class Directory:
    """Represents a directory in the inbox bucket ready for processing."""

    prefix: str  # e.g., "project-a/run-1/"
    checksum_file_key: str  # e.g., "project-a/run-1/abc123.CHECKSUM"
    checksum_file_hash: str  # The SHA256 from the filename (e.g., "abc123")
    discovered_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def checksum_id(self) -> str:
        """Return the checksum ID (hash from filename)."""
        return self.checksum_file_hash


@dataclass
class CorruptedFile:
    """Represents a file that failed checksum validation."""

    hash: str


@dataclass
class ValidationResult:
    """Result of checksum validation for a directory."""

    is_valid: bool
    corrupted_files: List[CorruptedFile]  # Files with mismatched checksums
    missing_hashes: int = 0  # Expected hashes with no matching file
    extra_files: int = 0  # Files with no matching expected hash

@dataclass
class TransferResult:
    """Result of transferring files from inbox to storage."""

    success: bool
    copied_objects: List[S3Location] = field(default_factory=list)
    failed_copies: List[S3Location] = field(default_factory=list)
    error_message: Optional[str] = None
