"""Tests for CleanupManager module."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.models import Directory
from src.cleanup import CleanupManager


@pytest.mark.asyncio
class TestCleanupManager:
    """Tests for CleanupManager."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        client = MagicMock()
        return client

    @pytest.fixture
    def cleanup_manager(self, mock_s3_client):
        """Create cleanup manager instance."""
        return CleanupManager(
            s3_client=mock_s3_client,
            inbox_bucket="test-inbox",
            batch_size=1000,
        )

    @pytest.fixture
    def test_directory(self):
        """Create test directory object."""
        return Directory(
            prefix="test-dir/",
            checksum_file_key="test-dir/abc123.CHECKSUM",
            checksum_file_hash="abc123",
        )

    async def test_cleanup_success(self, cleanup_manager, mock_s3_client, test_directory):
        """Test successful cleanup of directory."""
        # Mock listing objects
        async def mock_list_objects(bucket, prefix):
            yield MagicMock(key="test-dir/file1.dat", size=100)
            yield MagicMock(key="test-dir/file2.dat", size=200)
            yield MagicMock(key="test-dir/abc123.CHECKSUM", size=50)

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.delete_objects = AsyncMock()

        result = await cleanup_manager.cleanup(test_directory)

        assert result is True
        mock_s3_client.delete_objects.assert_called_once()

    async def test_cleanup_empty_directory(self, cleanup_manager, mock_s3_client, test_directory):
        """Test cleanup of empty directory."""
        async def mock_list_objects(bucket, prefix):
            return
            yield  # Make it a generator

        mock_s3_client.list_objects_v2 = mock_list_objects

        result = await cleanup_manager.cleanup(test_directory)

        assert result is True
        mock_s3_client.delete_objects.assert_not_called()

    async def test_cleanup_batching(self, cleanup_manager, mock_s3_client, test_directory):
        """Test cleanup batches deletes correctly."""
        # Create more than batch_size objects
        keys = [f"test-dir/file{i}.dat" for i in range(1500)]

        async def mock_list_objects(bucket, prefix):
            for key in keys:
                yield MagicMock(key=key, size=100)

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.delete_objects = AsyncMock()

        result = await cleanup_manager.cleanup(test_directory)

        assert result is True
        # Should be called twice (1000 + 500)
        assert mock_s3_client.delete_objects.call_count == 2

    async def test_cleanup_failure(self, cleanup_manager, mock_s3_client, test_directory):
        """Test cleanup handles errors."""
        async def mock_list_objects(bucket, prefix):
            yield MagicMock(key="test-dir/file1.dat", size=100)

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.delete_objects = AsyncMock(side_effect=Exception("Delete failed"))

        result = await cleanup_manager.cleanup(test_directory)

        assert result is False
