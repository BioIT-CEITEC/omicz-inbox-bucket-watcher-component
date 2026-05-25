"""Tests for ChecksumValidator module."""

import hashlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.models import CorruptedFile, Directory, ValidationResult
from src.validator import ChecksumValidator


class TestParseChecksumFile:
    """Tests for parse_checksum_file method."""

    def test_parse_valid_hashes(self):
        """Test parsing valid SHA-256 hashes."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = """e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8
6ca13d52ca70c883e0f0bb101e425a89e8624de51db2d2392593af6a84118090"""

        hashes = validator.parse_checksum_file(content)
        assert len(hashes) == 3
        assert "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" in hashes

    def test_parse_with_empty_lines(self):
        """Test parsing with empty lines."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = """e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8
"""
        hashes = validator.parse_checksum_file(content)
        assert len(hashes) == 2

    def test_parse_invalid_hash(self, caplog):
        """Test parsing with invalid hash format."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = """e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
invalid_hash
5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"""

        hashes = validator.parse_checksum_file(content)
        assert len(hashes) == 2
        assert "Invalid hash format" in caplog.text

    def test_parse_empty_content(self):
        """Test parsing empty content."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        hashes = validator.parse_checksum_file("")
        assert len(hashes) == 0


class TestVerifyFilenameHash:
    """Tests for verify_filename_hash method."""

    def test_valid_hash_match(self):
        """Test when filename hash matches content hash."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = "test content"
        expected_hash = hashlib.sha256(content.encode()).hexdigest()

        result = validator.verify_filename_hash(expected_hash, content)
        assert result is True

    def test_invalid_hash_mismatch(self):
        """Test when filename hash doesn't match content hash."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = "test content"
        wrong_hash = "0" * 64

        result = validator.verify_filename_hash(wrong_hash, content)
        assert result is False

    def test_case_insensitive(self):
        """Test hash comparison is case insensitive."""
        validator = ChecksumValidator(MagicMock(), inbox_bucket="test-inbox")
        content = "test content"
        expected_hash = hashlib.sha256(content.encode()).hexdigest()
        upper_hash = expected_hash.upper()

        result = validator.verify_filename_hash(upper_hash, content)
        assert result is True


@pytest.mark.asyncio
class TestValidate:
    """Tests for validate method."""

    @pytest.fixture
    def mock_s3_client(self):
        """Create mock S3 client."""
        return MagicMock()

    @pytest.fixture
    def test_directory(self):
        """Create test directory object."""
        content = b"test checksum content"
        checksum_hash = hashlib.sha256(content).hexdigest()
        return Directory(
            prefix="test-dir/",
            checksum_file_key="test-dir/abc123.CHECKSUM",
            checksum_file_hash=checksum_hash,
        )

    async def test_valid_directory(self, mock_s3_client, test_directory):
        """Test validation of valid directory."""
        content = b"test checksum content"
        checksum_hash = hashlib.sha256(content).hexdigest()

        # Mock CHECKSUM file content
        mock_s3_client.get_object_bytes = AsyncMock(return_value=content)

        # Mock listing data files
        async def mock_list_objects(bucket, prefix):
            yield MagicMock(key="test-dir/file1.dat", size=100)
            yield MagicMock(key="test-dir/file2.dat", size=200)

        mock_s3_client.list_objects_v2 = mock_list_objects

        # Mock file content and hash computation
        file1_content = b"file1 content"
        file1_hash = hashlib.sha256(file1_content).hexdigest()

        file2_content = b"file2 content"
        file2_hash = hashlib.sha256(file2_content).hexdigest()

        # Create checksum file with correct hashes
        checksum_content = f"{file1_hash}\n{file2_hash}".encode()
        new_checksum_hash = hashlib.sha256(checksum_content).hexdigest()
        test_directory.checksum_file_hash = new_checksum_hash
        mock_s3_client.get_object_bytes = AsyncMock(return_value=checksum_content)

        async def mock_get_object(bucket, key):
            if key == "test-dir/file1.dat":
                yield file1_content
            elif key == "test-dir/file2.dat":
                yield file2_content

        mock_s3_client.get_object = mock_get_object

        validator = ChecksumValidator(mock_s3_client, inbox_bucket="test-inbox")
        result = await validator.validate(test_directory)

        assert result.is_valid is True
        assert len(result.corrupted_files) == 0

    async def test_invalid_checksum_filename(self, mock_s3_client, test_directory):
        """Test validation fails when filename hash doesn't match content."""
        content = b"test checksum content"
        # Use wrong hash in directory
        test_directory.checksum_file_hash = "wrong" * 16

        mock_s3_client.get_object_bytes = AsyncMock(return_value=content)

        validator = ChecksumValidator(mock_s3_client, inbox_bucket="test-inbox")
        result = await validator.validate(test_directory)

        assert result.is_valid is False

    async def test_corrupted_file(self, mock_s3_client, test_directory):
        """Test validation detects corrupted file."""
        # Create checksum with specific hash
        expected_hash = "a" * 64
        checksum_content = f"{expected_hash}\n".encode()
        checksum_hash = hashlib.sha256(checksum_content).hexdigest()
        test_directory.checksum_file_hash = checksum_hash

        mock_s3_client.get_object_bytes = AsyncMock(return_value=checksum_content)

        # Mock file with wrong content (different hash)
        async def mock_list_objects(bucket, prefix):
            yield MagicMock(key="test-dir/file1.dat", size=100)

        mock_s3_client.list_objects_v2 = mock_list_objects

        async def mock_get_object(bucket, key):
            yield b"wrong content"

        mock_s3_client.get_object = mock_get_object

        validator = ChecksumValidator(mock_s3_client, inbox_bucket="test-inbox")
        result = await validator.validate(test_directory)

        assert result.is_valid is False
        assert len(result.corrupted_files) == 1
