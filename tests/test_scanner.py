"""Tests for DirectoryScanner module."""

import hashlib
import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

from src.models import Directory
from src.scanner import DirectoryScanner


@pytest.mark.asyncio
class TestDirectoryScanner:
    """Tests for DirectoryScanner."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        client = MagicMock()
        return client

    @pytest.fixture
    def scanner(self, mock_s3_client):
        """Create scanner instance."""
        return DirectoryScanner(
            s3_client=mock_s3_client,
            inbox_bucket="test-inbox",
            checksum_bucket="test-checksum",
            directories=["uploads/"],
            settling_period_seconds=30,
        )

    async def test_scan_finds_checksum_files(self, scanner, mock_s3_client):
        """Test scanner finds CHECKSUM files."""
        now = datetime.now(timezone.utc)
        last_modified = now - timedelta(minutes=5)

        # Mock S3Object-like objects
        obj1 = MagicMock(
            key="uploads/project-a/abc123.CHECKSUM",
            size=100,
            etag="etag1",
            last_modified=last_modified,
        )
        obj2 = MagicMock(
            key="uploads/project-b/def456.CHECKSUM",
            size=200,
            etag="etag2",
            last_modified=last_modified,
        )

        # Mock list_objects_v2 to return CHECKSUM files
        async def mock_list_objects(bucket, prefix):
            yield obj1
            yield obj2

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.object_exists = AsyncMock(return_value=False)

        directories = await scanner.scan()

        assert len(directories) == 2
        assert directories[0].prefix == "uploads/project-a/"
        assert directories[0].checksum_file_hash == "abc123"
        assert directories[1].checksum_file_hash == "def456"

    async def test_scan_skips_processed_directories(self, scanner, mock_s3_client):
        """Test scanner skips already processed directories."""
        now = datetime.now(timezone.utc)
        last_modified = now - timedelta(minutes=5)

        obj = MagicMock(
            key="uploads/project-a/abc123.CHECKSUM",
            size=100,
            etag="etag1",
            last_modified=last_modified,
        )

        async def mock_list_objects(bucket, prefix):
            yield obj

        mock_s3_client.list_objects_v2 = mock_list_objects
        # Success marker exists (empty file)
        mock_s3_client.object_exists = AsyncMock(return_value=True)
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=0)
        )

        directories = await scanner.scan()

        assert len(directories) == 0

    async def test_scan_includes_retry_candidates(self, scanner, mock_s3_client):
        """Test scanner includes directories with failure markers."""
        now = datetime.now(timezone.utc)
        last_modified = now - timedelta(minutes=5)

        obj = MagicMock(
            key="uploads/project-a/abc123.CHECKSUM",
            size=100,
            etag="etag1",
            last_modified=last_modified,
        )

        async def mock_list_objects(bucket, prefix):
            yield obj

        mock_s3_client.list_objects_v2 = mock_list_objects
        # Failure marker exists (non-empty file)
        mock_s3_client.object_exists = AsyncMock(return_value=True)
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=100)
        )

        directories = await scanner.scan()

        # Should include retry candidates
        assert len(directories) == 1

    async def test_scan_respects_settling_period(self, scanner, mock_s3_client):
        """Test scanner respects settling period."""
        now = datetime.now(timezone.utc)
        # This file is only 10 seconds old (less than 30s settling period)
        last_modified = now - timedelta(seconds=10)

        obj = MagicMock(
            key="uploads/project-a/abc123.CHECKSUM",
            size=100,
            etag="etag1",
            last_modified=last_modified,
        )

        async def mock_list_objects(bucket, prefix):
            yield obj

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.object_exists = AsyncMock(return_value=False)

        directories = await scanner.scan()

        assert len(directories) == 0

    async def test_scan_invalid_checksum_key_format(self, scanner, mock_s3_client, caplog):
        """Test scanner handles invalid CHECKSUM key format."""
        now = datetime.now(timezone.utc)
        last_modified = now - timedelta(minutes=5)

        obj = MagicMock(
            key="abc123.CHECKSUM",  # Invalid key without directory structure
            size=100,
            etag="etag1",
            last_modified=last_modified,
        )

        async def mock_list_objects(bucket, prefix):
            yield obj

        mock_s3_client.list_objects_v2 = mock_list_objects

        directories = await scanner.scan()

        assert len(directories) == 0
        assert "Invalid CHECKSUM key format" in caplog.text

    async def test_is_already_processed_success(self, scanner, mock_s3_client):
        """Test is_already_processed with success marker."""
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=0)
        )

        result = await scanner.is_already_processed("abc123")

        assert result is True

    async def test_is_already_processed_failure(self, scanner, mock_s3_client):
        """Test is_already_processed with failure marker."""
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=100)
        )

        result = await scanner.is_already_processed("abc123")

        assert result is False

    async def test_is_already_processed_not_found(self, scanner, mock_s3_client):
        """Test is_already_processed when marker doesn't exist."""
        mock_s3_client.head_object = AsyncMock(side_effect=Exception("Not found"))

        result = await scanner.is_already_processed("abc123")

        assert result is False
