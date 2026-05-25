"""Tests for ChecksumReporter module."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.models import CorruptedFile, Directory
from src.reporter import ChecksumReporter


@pytest.mark.asyncio
class TestChecksumReporter:
    """Tests for ChecksumReporter."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        client = MagicMock()
        return client

    @pytest.fixture
    def reporter(self, mock_s3_client, checksum_prefix=""):
        """Create checksum reporter instance."""
        return ChecksumReporter(
            s3_client=mock_s3_client,
            checksum_bucket="test-checksum",
            checksum_prefix=checksum_prefix,
        )

    @pytest.fixture
    def test_directory(self):
        """Create test directory object."""
        return Directory(
            prefix="test-dir/",
            checksum_file_key="test-dir/abc123.CHECKSUM",
            checksum_file_hash="abc123",
        )

    async def test_report_success(self, reporter, mock_s3_client, test_directory):
        """Test reporting success creates empty marker."""
        mock_s3_client.put_object = AsyncMock()

        result = await reporter.report_success(test_directory)

        assert result is True
        mock_s3_client.put_object.assert_called_once_with(
            bucket="test-checksum",
            key="abc123.CHECKSUM",
            body="",
            content_type="text/plain",
        )

    async def test_report_success_with_checksum_prefix(self, mock_s3_client, test_directory):
        """Test reporting success with checksum prefix creates marker in subdirectory."""
        reporter = ChecksumReporter(
            s3_client=mock_s3_client,
            checksum_bucket="test-checksum",
            checksum_prefix="checksums/",
        )
        mock_s3_client.put_object = AsyncMock()

        result = await reporter.report_success(test_directory)

        assert result is True
        mock_s3_client.put_object.assert_called_once_with(
            bucket="test-checksum",
            key="checksums/abc123.CHECKSUM",
            body="",
            content_type="text/plain",
        )

    async def test_report_success_failure(self, reporter, mock_s3_client, test_directory):
        """Test reporting success handles errors."""
        mock_s3_client.put_object = AsyncMock(side_effect=Exception("Put failed"))

        result = await reporter.report_success(test_directory)

        assert result is False

    async def test_report_failure(self, reporter, mock_s3_client, test_directory):
        """Test reporting failure creates marker with content."""
        mock_s3_client.put_object = AsyncMock()

        corrupted_files = [
            CorruptedFile(hash="hash1"),
            CorruptedFile(hash="hash2"),
        ]

        result = await reporter.report_failure(test_directory, corrupted_files)

        assert result is True
        mock_s3_client.put_object.assert_called_once()

    async def test_report_failure_with_expected_hashes(self, reporter, mock_s3_client, test_directory):
        """Test reporting failure with expected hashes."""
        mock_s3_client.put_object = AsyncMock()

        corrupted_files = [
            CorruptedFile(hash="hash1"),
            CorruptedFile(hash="hash2"),
            CorruptedFile(hash="hash3"),
        ]

        result = await reporter.report_failure(
            test_directory, corrupted_files
        )

        assert result is True
        call_args = mock_s3_client.put_object.call_args
        # Content should contain the corrupted file hashes
        assert "hash1" in call_args[1]["body"]

    async def test_report_failure_empty_corrupted(self, reporter, mock_s3_client, test_directory):
        """Test reporting failure with no corrupted files."""
        mock_s3_client.put_object = AsyncMock()

        result = await reporter.report_failure(test_directory, [])

        assert result is True
