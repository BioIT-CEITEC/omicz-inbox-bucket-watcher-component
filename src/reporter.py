"""Checksum Reporter module for writing success/failure markers to the checksum bucket."""

from __future__ import annotations

import logging
from typing import List

from .models import CorruptedFile, Directory
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class ChecksumReporter:
    """Creates marker files in the Checksum bucket.

    On success: creates an empty file `{checksum_id}.checksum`
    On failure: creates a file with newline-separated expected checksums of corrupted files
    """

    def __init__(
        self,
        s3_client: S3Client,
        checksum_bucket: str,
        checksum_prefix: str = "",
    ):
        """Initialize the checksum reporter.

        Args:
            s3_client: Async S3 client
            checksum_bucket: Bucket name for checksum markers
            checksum_prefix: Optional prefix to prepend to marker keys (e.g., "checksums/")
        """
        self.s3_client = s3_client
        self.checksum_bucket = checksum_bucket
        self.checksum_prefix = checksum_prefix

    async def report_success(self, directory: Directory) -> bool:
        """Create an empty success marker file in the checksum bucket.

        Args:
            directory: Directory object with checksum info

        Returns:
            True if report succeeded, False otherwise
        """
        marker_key = f"{self.checksum_prefix}{directory.checksum_id}.CHECKSUM"

        try:
            await self.s3_client.put_object(
                bucket=self.checksum_bucket,
                key=marker_key,
                body="",  # Empty file indicates success
                content_type="text/plain",
            )
            logger.info(f"Reported success for directory {directory.prefix} (marker: {marker_key})")
            return True

        except Exception as e:
            logger.error(f"Failed to report success for {directory.prefix}: {e}")
            return False

    async def report_failure(
        self,
        directory: Directory,
        corrupted_files: List[CorruptedFile],
    ) -> bool:
        """Create a failure marker file with corrupted file checksums.

        Args:
            directory: Directory object with checksum info
            corrupted_files: List of corrupted files
            expected_hashes: Optional set of expected hashes from the CHECKSUM file

        Returns:
            True if report succeeded, False otherwise
        """
        marker_key = f"{self.checksum_prefix}{directory.checksum_id}.CHECKSUM"

        if not corrupted_files or any(file.hash is None for file in corrupted_files):
            content = "VALIDATION_FAILED"
        else:
            content = "\n".join(file.hash for file in corrupted_files if file.hash is not None)

        try:
            await self.s3_client.put_object(
                bucket=self.checksum_bucket,
                key=marker_key,
                body=content,
                content_type="text/plain",
            )
            logger.warning(
                f"Reported failure for directory {directory.prefix} "
                f"({len(corrupted_files)} corrupted files, marker: {marker_key})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to report failure for {directory.prefix}: {e}")
            return False
