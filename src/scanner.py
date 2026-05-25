"""Directory Scanner module for discovering directories with checksum files."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from .models import Directory
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class DirectoryScanner:
    """Discovers directories in the Inbox bucket ready for processing.

    A directory is eligible for processing if:
    1. It contains a {sha256}.CHECKSUM file
    2. No empty marker exists in checksum bucket (not already succeeded)
    3. It has passed the settling period (S3 eventual consistency)
    """

    def __init__(
        self,
        s3_client: S3Client,
        inbox_bucket: str,
        checksum_bucket: str,
        directories: List[str],
        settling_period_seconds: int = 30,
    ):
        """Initialize the directory scanner.

        Args:
            s3_client: Async S3 client
            inbox_bucket: Name of the inbox bucket
            checksum_bucket: Name of the checksum bucket
            directories: List of directory prefixes to watch
            settling_period_seconds: Delay after discovering checksum file
        """
        self.s3_client = s3_client
        self.inbox_bucket = inbox_bucket
        self.checksum_bucket = checksum_bucket
        self.directories = directories
        self.settling_period_seconds = settling_period_seconds

    async def scan(self) -> List[Directory]:
        """Scan watched directories for directories containing .CHECKSUM files.

        Returns:
            List of Directory objects ready for processing
        """
        eligible_directories = []

        for prefix in self.directories:
            # Ensure prefix ends with /
            if not prefix.endswith("/"):
                prefix = prefix + "/"

            # Find all CHECKSUM files under this prefix
            async for obj in self.s3_client.list_objects_v2(
                self.inbox_bucket, prefix
            ):
                if obj.key.endswith(".CHECKSUM"):
                    directory = await self._process_checksum_file(obj.key, obj)
                    if directory:
                        eligible_directories.append(directory)

        return eligible_directories

    async def _process_checksum_file(self, checksum_key: str, obj) -> Optional[Directory]:
        """Process a discovered CHECKSUM file and determine if it's eligible.

        Args:
            checksum_key: S3 key of the CHECKSUM file

        Returns:
            Directory object if eligible, None otherwise
        """
        # Extract directory prefix and checksum hash from key
        # e.g., "project-a/run-1/abc123.CHECKSUM" -> prefix="project-a/run-1/", hash="abc123"
        parts = checksum_key.rsplit("/", 1)
        if len(parts) != 2:
            logger.warning(f"Invalid CHECKSUM key format: {checksum_key}")
            return None

        prefix = parts[0] + "/" if not parts[0].endswith("/") else parts[0]
        checksum_filename = parts[1]

        if not checksum_filename.endswith(".CHECKSUM"):
            return None

        checksum_hash = checksum_filename[:-9]  # Remove ".CHECKSUM"

        # Check if already processed (success marker exists)
        marker_key = f"{checksum_hash}.CHECKSUM"
        exists = await self.s3_client.object_exists(self.checksum_bucket, marker_key)

        if exists:
            # Check if it's a failure marker (non-empty file)
            try:
                metadata = await self.s3_client.head_object(self.checksum_bucket, marker_key)
                if metadata.content_length == 0:
                    # Success marker - skip this directory
                    logger.debug(f"Directory {prefix} already processed successfully")
                    return None
                else:
                    # Failure marker - this is a retry candidate
                    logger.info(f"Directory {prefix} has failure marker, will retry")
            except Exception:
                # Marker doesn't exist or error reading
                pass

        # Check settling period
        now = datetime.now(timezone.utc)
        time_since_discovery = (now - obj.last_modified.replace(tzinfo=timezone.utc)).total_seconds()

        if time_since_discovery < self.settling_period_seconds:
            logger.debug(
                f"Directory {prefix} not ready - settling period not elapsed "
                f"({time_since_discovery:.1f}s < {self.settling_period_seconds}s)"
            )
            return None

        return Directory(
            prefix=prefix,
            checksum_file_key=checksum_key,
            checksum_file_hash=checksum_hash,
            discovered_at=obj.last_modified,
        )

    async def is_already_processed(self, checksum_hash: str) -> bool:
        """Check if a directory has already been processed successfully.

        Args:
            checksum_hash: The SHA256 hash from the CHECKSUM filename

        Returns:
            True if successfully processed, False otherwise
        """
        marker_key = f"{checksum_hash}.checksum"
        try:
            metadata = await self.s3_client.head_object(self.checksum_bucket, marker_key)
            # Empty file means success
            return metadata.content_length == 0
        except Exception:
            return False
