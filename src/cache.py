"""Processing Cache module for tracking directories currently being processed.

In-memory cache for preventing duplicate concurrent processing within a single instance.
Entries are automatically evicted after a timeout to prevent stuck states.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class ProcessingCache:
    """In-memory cache for tracking directories currently being processed.

    This is intentionally ephemeral. On restart, the cache is empty and
    the service relies on S3 marker files to determine state.
    """

    def __init__(self, entry_timeout: int = 300):
        """Initialize the processing cache.

        Args:
            entry_timeout: Seconds after which an entry is considered stale
        """
        self._cache: Dict[str, float] = {}  # prefix -> timestamp
        self._lock = asyncio.Lock()
        self.entry_timeout = entry_timeout

    async def is_processing(self, directory_prefix: str) -> bool:
        """Check if a directory is currently being processed.

        Args:
            directory_prefix: The directory prefix to check

        Returns:
            True if directory is being processed, False otherwise
        """
        async with self._lock:
            if directory_prefix not in self._cache:
                return False

            # Check if entry has timed out
            timestamp = self._cache[directory_prefix]
            if time.time() - timestamp > self.entry_timeout:
                logger.warning(
                    f"Cache entry for {directory_prefix} timed out, removing"
                )
                del self._cache[directory_prefix]
                return False

            return True

    async def mark_processing(self, directory_prefix: str) -> None:
        """Mark a directory as currently being processed.

        Args:
            directory_prefix: The directory prefix to mark
        """
        async with self._lock:
            self._cache[directory_prefix] = time.time()
            logger.debug(f"Marked {directory_prefix} as processing")

    async def mark_completed(self, directory_prefix: str) -> None:
        """Mark a directory as completed and remove from processing set.

        Args:
            directory_prefix: The directory prefix to mark as completed
        """
        async with self._lock:
            if directory_prefix in self._cache:
                del self._cache[directory_prefix]
                logger.debug(f"Marked {directory_prefix} as completed")

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            logger.debug("Cleared all cache entries")

    async def cleanup_stale_entries(self) -> int:
        """Remove stale entries from the cache.

        Returns:
            Number of entries removed
        """
        async with self._lock:
            now = time.time()
            stale_keys = [
                key
                for key, timestamp in self._cache.items()
                if now - timestamp > self.entry_timeout
            ]
            for key in stale_keys:
                del self._cache[key]

            if stale_keys:
                logger.info(f"Cleaned up {len(stale_keys)} stale cache entries")

            return len(stale_keys)
