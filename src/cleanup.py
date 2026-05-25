"""Cleanup Manager module for removing processed directories from the inbox."""

from __future__ import annotations

import logging
from typing import List

from .models import Directory
from .s3_client import S3Client

logger = logging.getLogger(__name__)


class CleanupManager:
    """Removes successfully processed directories from the Inbox bucket.

    Uses S3 DeleteObjects for batch efficiency (max 1000 keys per request).
    Idempotent: safe to retry if some objects were already deleted.
    """

    BATCH_SIZE = 1000  # S3 DeleteObjects limit

    def __init__(
        self,
        s3_client: S3Client,
        inbox_bucket: str,
        batch_size: int = BATCH_SIZE,
    ):
        """Initialize the cleanup manager.

        Args:
            s3_client: Async S3 client
            inbox_bucket: Inbox bucket name
            batch_size: Number of keys to delete per batch request
        """
        self.s3_client = s3_client
        self.inbox_bucket = inbox_bucket
        self.batch_size = batch_size

    async def cleanup(self, directory: Directory) -> bool:
        """Delete all objects under the directory prefix.

        Args:
            directory: Directory object with prefix to clean up

        Returns:
            True if cleanup succeeded, False otherwise
        """
        try:
            # Collect all object keys under the prefix
            keys_to_delete: List[str] = []
            async for obj in self.s3_client.list_objects_v2(
                self.inbox_bucket, directory.prefix
            ):
                keys_to_delete.append(obj.key)

            if not keys_to_delete:
                logger.debug(f"No objects to delete in directory: {directory.prefix}")
                return True

            logger.info(f"Deleting {len(keys_to_delete)} objects from {directory.prefix}")

            # Delete in batches
            for i in range(0, len(keys_to_delete), self.batch_size):
                batch = keys_to_delete[i : i + self.batch_size]
                await self.s3_client.delete_objects(self.inbox_bucket, batch)
                logger.debug(f"Deleted batch of {len(batch)} objects")

            logger.info(f"Successfully cleaned up directory: {directory.prefix}")
            return True

        except Exception as e:
            logger.error(f"Failed to cleanup directory {directory.prefix}: {e}")
            return False
