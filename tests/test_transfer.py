"""Tests for TransferManager module."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.models import Directory, S3Location, TransferResult
from src.transfer import TransferManager


@pytest.mark.asyncio
class TestTransferManager:
    """Tests for TransferManager."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        client = MagicMock()
        return client

    @pytest.fixture
    def transfer_manager(self, mock_s3_client):
        """Create transfer manager instance without storage prefix."""
        return TransferManager(
            s3_client=mock_s3_client,
            storage_bucket="test-storage",
            max_concurrent_copies=5,
            verify_after_copy=True,
            storage_prefix="",
            inbox_prefix="",
        )

    @pytest.fixture
    def transfer_manager_with_prefix(self, mock_s3_client):
        """Create transfer manager instance with storage prefix."""
        return TransferManager(
            s3_client=mock_s3_client,
            storage_bucket="test-storage",
            max_concurrent_copies=5,
            verify_after_copy=True,
            storage_prefix="storage/",
            inbox_prefix="",
        )

    @pytest.fixture
    def transfer_manager_with_inbox_prefix(self, mock_s3_client):
        """Create transfer manager instance with inbox_prefix."""
        return TransferManager(
            s3_client=mock_s3_client,
            storage_bucket="test-storage",
            max_concurrent_copies=5,
            verify_after_copy=True,
            storage_prefix="storage/",
            inbox_prefix="inbox/",
        )

    @pytest.fixture
    def test_directory(self):
        """Create test directory object."""
        return Directory(
            prefix="test-dir/",
            checksum_file_key="test-dir/abc123.CHECKSUM",
            checksum_file_hash="abc123",
        )

    async def test_transfer_success(self, transfer_manager, mock_s3_client, test_directory):
        """Test successful transfer of all files."""
        # Mock listing objects
        async def mock_list_objects(bucket, prefix):
            if bucket == "test-storage":
                # Destination is empty
                return
            yield MagicMock(key="test-dir/file1.dat", size=100)
            yield MagicMock(key="test-dir/file2.dat", size=200)

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.copy_object = AsyncMock()
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=100, etag="etag1")
        )

        result = await transfer_manager.transfer(test_directory, "test-inbox")

        assert result.success is True
        assert len(result.copied_objects) == 2
        assert len(result.failed_copies) == 0

    async def test_transfer_empty_directory(self, transfer_manager, mock_s3_client, test_directory):
        """Test transfer of empty directory."""
        async def mock_list_objects(bucket, prefix):
            # No objects yielded - destination empty, then inbox empty
            return
            yield  # Makes this an async generator

        mock_s3_client.list_objects_v2 = mock_list_objects

        result = await transfer_manager.transfer(test_directory, "test-inbox")

        assert result.success is True
        assert len(result.copied_objects) == 0

    async def test_transfer_partial_failure(self, transfer_manager, mock_s3_client, test_directory):
        """Test transfer with some failures."""
        async def mock_list_objects(bucket, prefix):
            if bucket == "test-storage":
                # Destination is empty
                return
            yield MagicMock(key="test-dir/file1.dat", size=100)
            yield MagicMock(key="test-dir/file2.dat", size=200)

        mock_s3_client.list_objects_v2 = mock_list_objects

        # First copy succeeds, second fails
        call_count = [0]

        async def mock_copy_object(source_bucket, source_key, dest_bucket, dest_key):
            call_count[0] += 1
            if call_count[0] == 2:
                raise Exception("Copy failed")

        mock_s3_client.copy_object = mock_copy_object
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=100, etag="etag1")
        )

        result = await transfer_manager.transfer(test_directory, "test-inbox")

        assert result.success is False
        assert len(result.copied_objects) == 1
        assert len(result.failed_copies) == 1

    async def test_verify_copy_success(self, transfer_manager, mock_s3_client):
        """Test copy verification succeeds."""
        source_meta = MagicMock(content_length=100, etag="etag1")
        dest_meta = MagicMock(content_length=100, etag="etag1")

        async def mock_head_object(bucket, key):
            return source_meta if bucket == "test-inbox" else dest_meta

        mock_s3_client.head_object = mock_head_object

        result = await transfer_manager._verify_copy_by_dest_key("test-inbox", "test-dir/file1.dat", "test-dir/file1.dat")

        assert result is True

    async def test_verify_copy_size_mismatch(self, transfer_manager, mock_s3_client):
        """Test copy verification fails on size mismatch."""
        source_meta = MagicMock(content_length=100, etag="etag1")
        dest_meta = MagicMock(content_length=200, etag="etag1")

        async def mock_head_object(bucket, key):
            return source_meta if bucket == "test-inbox" else dest_meta

        mock_s3_client.head_object = mock_head_object

        result = await transfer_manager._verify_copy_by_dest_key("test-inbox", "test-dir/file1.dat", "test-dir/file1.dat")

        assert result is False

    async def test_compute_dest_key_no_prefix(self, transfer_manager):
        """Test destination key computation without any prefixes."""
        # With empty inbox_prefix, the full source key is used
        dest_key = transfer_manager._compute_dest_key("test-dir/file.dat", "test-dir/")
        assert dest_key == "test-dir/file.dat"

    async def test_compute_dest_key_with_storage_prefix(self, transfer_manager_with_prefix):
        """Test destination key computation with storage prefix only."""
        dest_key = transfer_manager_with_prefix._compute_dest_key("project-a/file.dat", "project-a/")
        assert dest_key == "storage/project-a/file.dat"

    async def test_compute_dest_key_with_inbox_prefix(self, transfer_manager_with_inbox_prefix):
        """Test destination key computation with inbox_prefix stripped."""
        dest_key = transfer_manager_with_inbox_prefix._compute_dest_key("inbox/project-a/file.dat", "project-a/")
        assert dest_key == "storage/project-a/file.dat"

    async def test_compute_dest_key_nested_path_with_inbox_prefix(self, transfer_manager_with_inbox_prefix):
        """Test destination key computation with nested paths and inbox_prefix."""
        dest_key = transfer_manager_with_inbox_prefix._compute_dest_key("inbox/deep/nested/path.dat", "deep/")
        assert dest_key == "storage/deep/nested/path.dat"

    async def test_compute_dest_key_no_matching_inbox_prefix(self, transfer_manager_with_inbox_prefix):
        """Test destination key when source doesn't match inbox_prefix (fallback behavior)."""
        dest_key = transfer_manager_with_inbox_prefix._compute_dest_key("other/file.dat", "other/")
        assert dest_key == "storage/other/file.dat"

    async def test_compute_dest_key_empty_inbox_prefix(self, transfer_manager_with_prefix):
        """Test destination key with empty inbox_prefix (backward compatible)."""
        # transfer_manager_with_prefix has empty inbox_prefix
        dest_key = transfer_manager_with_prefix._compute_dest_key("project-a/file.dat", "project-a/")
        assert dest_key == "storage/project-a/file.dat"

    async def test_transfer_with_prefix_rewrite(self, transfer_manager_with_inbox_prefix, mock_s3_client, test_directory):
        """Test successful transfer with inbox_prefix and storage_prefix rewriting."""
        # Create a directory with inbox/ prefix to match inbox_prefix
        test_dir = Directory(
            prefix="inbox/test-dir/",
            checksum_file_key="inbox/test-dir/abc123.CHECKSUM",
            checksum_file_hash="abc123",
        )

        # Mock listing objects with inbox/ prefix
        async def mock_list_objects(bucket, prefix):
            if bucket == "test-storage":
                # Destination is empty
                return
            yield MagicMock(key="inbox/test-dir/file1.dat", size=100)
            yield MagicMock(key="inbox/test-dir/file2.dat", size=200)

        mock_s3_client.list_objects_v2 = mock_list_objects
        mock_s3_client.copy_object = AsyncMock()
        mock_s3_client.head_object = AsyncMock(
            return_value=MagicMock(content_length=100, etag="etag1")
        )

        result = await transfer_manager_with_inbox_prefix.transfer(test_dir, "test-inbox")

        assert result.success is True
        assert len(result.copied_objects) == 2
        # Verify destination keys have inbox_prefix stripped and storage_prefix prepended
        assert result.copied_objects[0].key == "storage/test-dir/file1.dat"
        assert result.copied_objects[1].key == "storage/test-dir/file2.dat"

    async def test_verify_copy_by_dest_key_success(self, transfer_manager, mock_s3_client):
        """Test copy verification with different source and dest keys."""
        source_meta = MagicMock(content_length=100, etag="etag1")
        dest_meta = MagicMock(content_length=100, etag="etag1")

        async def mock_head_object(bucket, key):
            return source_meta if bucket == "test-inbox" else dest_meta

        mock_s3_client.head_object = mock_head_object

        result = await transfer_manager._verify_copy_by_dest_key("test-inbox", "inbox/file.dat", "storage/file.dat")

        assert result is True

    async def test_verify_copy_by_dest_key_size_mismatch(self, transfer_manager, mock_s3_client):
        """Test verification fails on size mismatch with different keys."""
        source_meta = MagicMock(content_length=100, etag="etag1")
        dest_meta = MagicMock(content_length=200, etag="etag1")

        async def mock_head_object(bucket, key):
            return source_meta if bucket == "test-inbox" else dest_meta

        mock_s3_client.head_object = mock_head_object

        result = await transfer_manager._verify_copy_by_dest_key("test-inbox", "inbox/file.dat", "storage/file.dat")

        assert result is False

    async def test_verify_copy_etag_mismatch(self, transfer_manager, mock_s3_client):
        """Test copy verification fails on ETag mismatch."""
        source_meta = MagicMock(content_length=100, etag="etag1")
        dest_meta = MagicMock(content_length=100, etag="etag2")

        async def mock_head_object(bucket, key):
            return source_meta if bucket == "test-inbox" else dest_meta

        mock_s3_client.head_object = mock_head_object

        result = await transfer_manager._verify_copy_by_dest_key("test-inbox", "test-dir/file1.dat", "test-dir/file1.dat")

        assert result is False

    async def test_transfer_skips_when_destination_exists(self, transfer_manager, mock_s3_client, test_directory):
        """Test that transfer is skipped when destination directory already exists."""
        # Mock destination listing to return an object (destination exists)
        async def mock_list_objects_dest(bucket, prefix):
            if bucket == "test-storage":
                yield MagicMock(key="test-dir/file1.dat", size=100)
            else:
                yield MagicMock(key="test-dir/file1.dat", size=100)
                yield MagicMock(key="test-dir/file2.dat", size=200)

        mock_s3_client.list_objects_v2 = mock_list_objects_dest
        mock_s3_client.copy_object = AsyncMock()

        result = await transfer_manager.transfer(test_directory, "test-inbox")

        assert result.success is False
        assert len(result.copied_objects) == 0
        assert len(result.failed_copies) == 0
        assert result.error_message is not None
        assert "already exists" in result.error_message
        mock_s3_client.copy_object.assert_not_called()
