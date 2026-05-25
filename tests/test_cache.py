"""Tests for ProcessingCache module."""

import asyncio
import pytest
import time

from src.cache import ProcessingCache


@pytest.mark.asyncio
class TestProcessingCache:
    """Tests for ProcessingCache."""

    @pytest.fixture
    def cache(self):
        """Create cache instance with short timeout for testing."""
        return ProcessingCache(entry_timeout=1)

    async def test_is_processing_empty_cache(self, cache):
        """Test is_processing returns False for empty cache."""
        result = await cache.is_processing("test-prefix")
        assert result is False

    async def test_mark_processing(self, cache):
        """Test marking directory as processing."""
        await cache.mark_processing("test-prefix")
        result = await cache.is_processing("test-prefix")
        assert result is True

    async def test_mark_completed(self, cache):
        """Test marking directory as completed."""
        await cache.mark_processing("test-prefix")
        await cache.mark_completed("test-prefix")
        result = await cache.is_processing("test-prefix")
        assert result is False

    async def test_entry_timeout(self, cache):
        """Test entries timeout after configured period."""
        await cache.mark_processing("test-prefix")

        # Should be processing immediately
        assert await cache.is_processing("test-prefix") is True

        # Wait for timeout
        await asyncio.sleep(1.1)

        # Should no longer be processing
        assert await cache.is_processing("test-prefix") is False

    async def test_clear(self, cache):
        """Test clearing all cache entries."""
        await cache.mark_processing("prefix1")
        await cache.mark_processing("prefix2")
        await cache.mark_processing("prefix3")

        await cache.clear()

        assert await cache.is_processing("prefix1") is False
        assert await cache.is_processing("prefix2") is False
        assert await cache.is_processing("prefix3") is False

    async def test_cleanup_stale_entries(self, cache):
        """Test cleaning up stale entries."""
        await cache.mark_processing("prefix1")
        await cache.mark_processing("prefix2")

        # Wait for timeout
        await asyncio.sleep(1.1)

        # Mark another as processing (not stale)
        await cache.mark_processing("prefix3")

        # Clean up stale entries
        removed = await cache.cleanup_stale_entries()

        assert removed == 2
        assert await cache.is_processing("prefix1") is False
        assert await cache.is_processing("prefix2") is False
        assert await cache.is_processing("prefix3") is True

    async def test_concurrent_access(self, cache):
        """Test thread-safe concurrent access."""
        async def mark_and_check(prefix):
            await cache.mark_processing(prefix)
            assert await cache.is_processing(prefix) is True
            await cache.mark_completed(prefix)
            assert await cache.is_processing(prefix) is False

        # Run multiple concurrent operations
        tasks = [mark_and_check(f"prefix{i}") for i in range(10)]
        await asyncio.gather(*tasks)
