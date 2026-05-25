"""Transfer Manager module for copying files from inbox to storage bucket."""

from __future__ import annotations

import asyncio
import logging
from typing import List

from .models import Directory, S3Location, TransferResult
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class TransferManager:
    """Copies validated directories from Inbox to Storage bucket.

    Uses S3 server-side copy operations (not download+upload) for efficiency.
    Supports rewriting destination keys with a storage prefix.
    """

    def __init__(
        self,
        s3_client: S3Client,
        storage_bucket: str,
        max_concurrent_copies: int = 10,
        verify_after_copy: bool = True,
        storage_prefix: str = "",
        inbox_prefix: str = "",
    ):
        """Initialize the transfer manager.

        Args:
            s3_client: Async S3 client
            storage_bucket: Destination bucket for validated data
            max_concurrent_copies: Maximum concurrent copy operations
            verify_after_copy: Verify objects after copying via HEAD request
            storage_prefix: Prefix to prepend to destination keys (default: "")
            inbox_prefix: Prefix to strip from source keys (default: "")
        """
        self.s3_client = s3_client
        self.storage_bucket = storage_bucket
        self.max_concurrent_copies = max_concurrent_copies
        self.verify_after_copy = verify_after_copy
        self.storage_prefix = storage_prefix
        self.inbox_prefix = inbox_prefix

    async def transfer(self, directory: Directory, inbox_bucket: str) -> TransferResult:
        """Copy all data files from inbox to storage bucket.

        Args:
            directory: Directory object with prefix info
            inbox_bucket: Source inbox bucket name

        Returns:
            TransferResult with success status and copied objects
        """
        copied_objects: List[S3Location] = []
        failed_copies: List[S3Location] = []

        # Check if destination already exists
        dest_prefix = self._compute_dest_key(directory.prefix, directory.prefix)
        dest_exists = False
        async for _ in self.s3_client.list_objects_v2(self.storage_bucket, dest_prefix):
            dest_exists = True
            break

        if dest_exists:
            logger.warning(f"Destination already exists, skipping transfer: {dest_prefix}")
            return TransferResult(
                success=False,
                copied_objects=[],
                failed_copies=[],
                error_message=f"Destination already exists: {dest_prefix}",
            )

        # List all objects under the directory prefix (excluding CHECKSUM file)
        objects_to_copy: List[str] = []
        async for obj in self.s3_client.list_objects_v2(inbox_bucket, directory.prefix):
            if not obj.key.endswith(".CHECKSUM"):
                objects_to_copy.append(obj.key)

        if not objects_to_copy:
            logger.warning(f"No objects to transfer in directory: {directory.prefix}")
            return TransferResult(
                success=True,
                copied_objects=[],
                failed_copies=[],
            )

        logger.info(f"Transferring {len(objects_to_copy)} objects from {directory.prefix}")

        # Copy objects with concurrency limit
        semaphore = asyncio.Semaphore(self.max_concurrent_copies)

        async def copy_with_semaphore(source_key: str):
            async with semaphore:
                return await self._copy_object(inbox_bucket, source_key, directory.prefix)

        # Create tasks for all objects
        tasks = [copy_with_semaphore(key) for key in objects_to_copy]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for source_key, result in zip(objects_to_copy, results):
            dest_key = self._compute_dest_key(source_key, directory.prefix)
            if isinstance(result, Exception):
                logger.error(f"Failed to copy {source_key}: {result}")
                failed_copies.append(S3Location(bucket=self.storage_bucket, key=dest_key))
            elif result:
                copied_objects.append(S3Location(bucket=self.storage_bucket, key=dest_key))
            else:
                failed_copies.append(S3Location(bucket=self.storage_bucket, key=dest_key))

        success = len(failed_copies) == 0

        if success:
            logger.info(
                f"Successfully transferred {len(copied_objects)} objects to {self.storage_bucket}"
            )
        else:
            logger.error(
                f"Transfer partially failed: {len(copied_objects)} succeeded, "
                f"{len(failed_copies)} failed"
            )

        return TransferResult(
            success=success,
            copied_objects=copied_objects,
            failed_copies=failed_copies,
        )

    def _compute_dest_key(self, source_key: str, directory_prefix: str) -> str:
        """Compute destination key by rewriting prefix.

        Args:
            source_key: Full source object key (e.g., "inbox/project-a/file.dat")
            directory_prefix: The watched directory prefix (e.g., "inbox/")

        Returns:
            Destination key with storage_prefix prepended after stripping inbox_prefix
            (e.g., "storage/project-a/file.dat" if storage_prefix="storage/" and inbox_prefix="inbox/")
        """
        # Strip the inbox prefix from source key
        if self.inbox_prefix and source_key.startswith(self.inbox_prefix):
            relative_key = source_key.removeprefix(self.inbox_prefix)
        else:
            relative_key = source_key

        # Prepend storage prefix if configured
        if self.storage_prefix:
            return f"{self.storage_prefix}{relative_key}"
        return relative_key

    async def _copy_object(self, source_bucket: str, source_key: str, directory_prefix: str) -> bool:
        """Copy a single object from source to storage bucket.

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            directory_prefix: The watched directory prefix for computing dest key

        Returns:
            True if copy succeeded and verified, False otherwise
        """
        dest_key = self._compute_dest_key(source_key, directory_prefix)

        try:
            # Server-side copy
            await self.s3_client.copy_object(
                source_bucket=source_bucket,
                source_key=source_key,
                dest_bucket=self.storage_bucket,
                dest_key=dest_key,
            )

            # Verify the copy if enabled
            if self.verify_after_copy:
                return await self._verify_copy_by_dest_key(source_bucket, source_key, dest_key)

            return True

        except Exception as e:
            logger.error(f"Error copying {source_key} to {dest_key}: {e}")
            return False

    async def _verify_copy_by_dest_key(self, source_bucket: str, source_key: str, dest_key: str) -> bool:
        """Verify that a copied object matches the source.

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_key: Destination object key

        Returns:
            True if verification passed, False otherwise
        """
        try:
            source_meta = await self.s3_client.head_object(source_bucket, source_key)
            dest_meta = await self.s3_client.head_object(self.storage_bucket, dest_key)

            # Compare size and ETag
            if source_meta.content_length != dest_meta.content_length:
                logger.error(
                    f"Size mismatch for {source_key} -> {dest_key}: "
                    f"source={source_meta.content_length}, dest={dest_meta.content_length}"
                )
                return False

            if source_meta.etag != dest_meta.etag:
                logger.error(
                    f"ETag mismatch for {source_key} -> {dest_key}: "
                    f"source={source_meta.etag}, dest={dest_meta.etag}"
                )
                return False

            logger.debug(f"Verified copy of {source_key} -> {dest_key}")
            return True

        except Exception as e:
            logger.error(f"Verification failed for {dest_key}: {e}")
            return False
