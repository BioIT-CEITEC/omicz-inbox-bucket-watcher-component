"""Tests for Orchestrator module."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from src.models import CorruptedFile, Directory, ValidationResult, TransferResult
from src.orchestrator import Orchestrator


@pytest.mark.asyncio
class TestOrchestrator:
    """Tests for Orchestrator."""

    @pytest.fixture
    def mock_components(self):
        """Create mock components."""
        return {
            "scanner": AsyncMock(),
            "validator": AsyncMock(),
            "transfer_manager": AsyncMock(),
            "cleanup_manager": AsyncMock(),
            "reporter": AsyncMock(),
            "cache": AsyncMock(),
        }

    @pytest.fixture
    def orchestrator(self, mock_components):
        """Create orchestrator instance."""
        return Orchestrator(
            scanner=mock_components["scanner"],
            validator=mock_components["validator"],
            transfer_manager=mock_components["transfer_manager"],
            cleanup_manager=mock_components["cleanup_manager"],
            reporter=mock_components["reporter"],
            cache=mock_components["cache"],
            inbox_bucket="test-inbox",
            max_concurrent_directories=2,
        )

    @pytest.fixture
    def test_directory(self):
        """Create test directory object."""
        return Directory(
            prefix="test-dir/",
            checksum_file_key="test-dir/abc123.CHECKSUM",
            checksum_file_hash="abc123",
        )

    async def test_process_all_directories_empty(self, orchestrator, mock_components):
        """Test processing when no directories found."""
        mock_components["scanner"].scan = AsyncMock(return_value=[])

        stats = await orchestrator.process_all_directories()

        assert stats["discovered"] == 0
        assert stats["processed"] == 0

    async def test_process_directory_success(self, orchestrator, mock_components, test_directory):
        """Test successful directory processing."""
        mock_components["scanner"].scan = AsyncMock(return_value=[test_directory])
        mock_components["cache"].is_processing = AsyncMock(return_value=False)
        mock_components["cache"].mark_processing = AsyncMock()
        mock_components["cache"].mark_completed = AsyncMock()

        # Valid validation result
        mock_components["validator"].validate = AsyncMock(
            return_value=ValidationResult(
                is_valid=True,
                corrupted_files=[],
            )
        )

        # Successful transfer
        mock_components["transfer_manager"].transfer = AsyncMock(
            return_value=TransferResult(
                success=True,
                copied_objects=[],
                failed_copies=[],
            )
        )

        # Successful cleanup
        mock_components["cleanup_manager"].cleanup = AsyncMock(return_value=True)

        # Successful report
        mock_components["reporter"].report_success = AsyncMock(return_value=True)

        stats = await orchestrator.process_all_directories()

        assert stats["discovered"] == 1
        assert stats["succeeded"] == 1
        assert stats["failed"] == 0

    async def test_process_directory_validation_failure(
        self, orchestrator, mock_components, test_directory
    ):
        """Test directory processing with validation failure."""
        mock_components["scanner"].scan = AsyncMock(return_value=[test_directory])
        mock_components["cache"].is_processing = AsyncMock(return_value=False)
        mock_components["cache"].mark_processing = AsyncMock()
        mock_components["cache"].mark_completed = AsyncMock()

        # Invalid validation result
        mock_components["validator"].validate = AsyncMock(
            return_value=ValidationResult(
                is_valid=False,
                corrupted_files=[
                    CorruptedFile(hash="hash1")
                ],
            )
        )

        # Should report failure
        mock_components["reporter"].report_failure = AsyncMock(return_value=True)

        stats = await orchestrator.process_all_directories()

        assert stats["discovered"] == 1
        assert stats["failed"] == 1
        mock_components["reporter"].report_failure.assert_called_once()

    async def test_process_directory_already_processing(
        self, orchestrator, mock_components, test_directory
    ):
        """Test skipping directory already being processed."""
        mock_components["scanner"].scan = AsyncMock(return_value=[test_directory])
        mock_components["cache"].is_processing = AsyncMock(return_value=True)

        stats = await orchestrator.process_all_directories()

        assert stats["discovered"] == 1
        assert stats["skipped"] == 1
        mock_components["validator"].validate.assert_not_called()

    async def test_process_directory_transfer_failure(
        self, orchestrator, mock_components, test_directory
    ):
        """Test directory processing with transfer failure."""
        mock_components["scanner"].scan = AsyncMock(return_value=[test_directory])
        mock_components["cache"].is_processing = AsyncMock(return_value=False)
        mock_components["cache"].mark_processing = AsyncMock()
        mock_components["cache"].mark_completed = AsyncMock()

        # Valid validation
        mock_components["validator"].validate = AsyncMock(
            return_value=ValidationResult(
                is_valid=True,
                corrupted_files=[],
            )
        )

        # Failed transfer
        mock_components["transfer_manager"].transfer = AsyncMock(
            return_value=TransferResult(
                success=False,
                copied_objects=[],
                failed_copies=[],
                error_message="Transfer failed",
            )
        )

        stats = await orchestrator.process_all_directories()

        assert stats["discovered"] == 1
        assert stats["failed"] == 1

    async def test_process_directory_exception_handling(
        self, orchestrator, mock_components, test_directory
    ):
        """Test exception handling during processing.
        
        Note: Exceptions caught inside process_directory are treated as failures,
        not errors. The error count is for exceptions in process_all_directories itself.
        """
        mock_components["scanner"].scan = AsyncMock(return_value=[test_directory])
        mock_components["cache"].is_processing = AsyncMock(return_value=False)
        mock_components["cache"].mark_processing = AsyncMock()
        mock_components["cache"].mark_completed = AsyncMock()

        # Validator raises exception - this gets caught and treated as failure
        mock_components["validator"].validate = AsyncMock(
            side_effect=Exception("Unexpected error")
        )

        stats = await orchestrator.process_all_directories()

        # Exception is caught inside process_directory and treated as failure
        assert stats["failed"] == 1
        mock_components["cache"].mark_completed.assert_called_once()
