"""Checksum Validator module for set-based SHA-256 validation."""

from __future__ import annotations

import hashlib
import logging
import re
from typing import List, Set, Tuple

from .models import CorruptedFile, Directory, ValidationResult
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class ChecksumValidator:
    """Parses SHA-256 checksum manifests and validates S3 object integrity.

    Uses set-based comparison: computes hashes of all files and compares
    against the set of expected hashes from the .CHECKSUM file.
    """

    # SHA-256 hash is 64 hex characters
    HASH_PATTERN = re.compile(r"^[a-fA-F0-9]{64}$")
    CHUNK_SIZE = 8 * 1024 * 1024  # 8MB chunks for streaming

    def __init__(
        self,
        s3_client: S3Client,
        inbox_bucket: str,
        chunk_size: int = CHUNK_SIZE,
    ):
        """Initialize the checksum validator.

        Args:
            s3_client: Async S3 client
            inbox_bucket: Name of the S3 inbox bucket
            chunk_size: Size of chunks for streaming file downloads
        """
        self.s3_client = s3_client
        self.inbox_bucket = inbox_bucket
        self.chunk_size = chunk_size

    async def validate(self, directory: Directory) -> ValidationResult:
        """Validate all files in a directory against the checksum manifest.

        Args:
            directory: Directory object with checksum file info

        Returns:
            ValidationResult with validation status and details
        """
        # Download and parse the CHECKSUM file
        try:
            checksum_content = await self.s3_client.get_object_bytes(
                self.inbox_bucket, directory.checksum_file_key
            )
        except Exception as e:
            logger.error(f"Failed to download CHECKSUM file: {e}")
            return ValidationResult(
                is_valid=False,
                corrupted_files=[],
            )

        # Verify the filename hash matches content hash
        content_hash = hashlib.sha256(checksum_content).hexdigest()
        if content_hash != directory.checksum_file_hash:
            logger.error(
                f"CHECKSUM filename hash mismatch: "
                f"expected={directory.checksum_file_hash}, actual={content_hash}"
            )
            return ValidationResult(
                is_valid=False,
                corrupted_files=[],
            )

        # Parse expected hashes
        expected_hashes = self.parse_checksum_file(checksum_content.decode("utf-8"))

        if not expected_hashes:
            logger.warning(f"No hashes found in CHECKSUM file: {directory.checksum_file_key}")
            return ValidationResult(
                is_valid=False,
                corrupted_files=[],
            )

        # List all data files in the directory (excluding the CHECKSUM file)
        data_files: List[str] = []
        async for obj in self.s3_client.list_objects_v2(
            self.inbox_bucket, directory.prefix
        ):
            if obj.key != directory.checksum_file_key:
                data_files.append(obj.key)

        # Compute hashes for all data files
        computed_hashes: Set[str] = set()

        for file_key in data_files:
            try:
                file_hash = await self._compute_file_hash(file_key)
                computed_hashes.add(file_hash)
            except Exception as e:
                logger.error(f"Failed to compute hash for {file_key}: {e}")

        corrupted_file_hashes = expected_hashes - computed_hashes
        corrupted_files = [CorruptedFile(hash=hash) for hash in corrupted_file_hashes]

        # Calculate missing and extra
        missing_hashes = len(expected_hashes - computed_hashes)
        extra_files = len(computed_hashes - expected_hashes)

        # Directory is valid only if:
        # 1. All computed hashes are in expected set
        # 2. Number of files equals number of expected hashes
        is_valid = (
            len(corrupted_files) == 0
            and missing_hashes == 0
            and extra_files == 0
        )

        return ValidationResult(
            is_valid=is_valid,
            corrupted_files=corrupted_files,
            missing_hashes=missing_hashes,
            extra_files=extra_files,
        )

    def parse_checksum_file(self, content: str) -> Set[str]:
        """Parse CHECKSUM file content into a set of expected SHA-256 hashes.

        Args:
            content: CHECKSUM file content (one hash per line)

        Returns:
            Set of valid SHA-256 hashes
        """
        hashes = set()
        for line in content.strip().split("\n"):
            line = line.strip()
            if line and self.HASH_PATTERN.match(line):
                hashes.add(line.lower())
            elif line:
                logger.warning(f"Invalid hash format in CHECKSUM file: {line}")
        return hashes

    def verify_filename_hash(self, checksum_id: str, content: str) -> bool:
        """Verify that the SHA-256 hash of the CHECKSUM file content matches the filename.

        Args:
            checksum_id: Hash from the filename
            content: CHECKSUM file content

        Returns:
            True if hash matches, False otherwise
        """
        computed_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
        return computed_hash.lower() == checksum_id.lower()

    async def _compute_file_hash(self, file_key: str) -> str:
        """Compute SHA-256 hash of an S3 object by streaming.

        Args:
            file_key: S3 key of the file

        Returns:
            SHA-256 hash as hex string
        """
        sha256_hash = hashlib.sha256()

        async for chunk in self.s3_client.get_object(
            self.inbox_bucket, file_key
        ):
            sha256_hash.update(chunk)

        return sha256_hash.hexdigest()
