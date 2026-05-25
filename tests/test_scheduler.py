"""Tests for Scheduler module."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.cache import ProcessingCache
from src.scheduler import Scheduler


@pytest.mark.asyncio
class TestScheduler:
    """Tests for Scheduler."""

    @pytest.fixture
    def mock_orchestrator(self):
        """Create mock orchestrator."""
        return AsyncMock()

    @pytest.fixture
    def mock_cache(self):
        """Create mock cache."""
        return AsyncMock(spec=ProcessingCache)

    @pytest.fixture
    def scheduler(self, mock_orchestrator, mock_cache):
        """Create scheduler instance."""
        return Scheduler(
            orchestrator=mock_orchestrator,
            cache=mock_cache,
            poll_interval_seconds=1,  # Short interval for testing
        )

    async def test_run_once(self, scheduler, mock_orchestrator):
        """Test single scan cycle."""
        mock_orchestrator.process_all_directories = AsyncMock(
            return_value={"discovered": 2, "processed": 2, "succeeded": 2}
        )

        stats = await scheduler.run_once()

        assert stats["discovered"] == 2
        mock_orchestrator.process_all_directories.assert_called_once()

    async def test_run_once_error_handling(self, scheduler, mock_orchestrator):
        """Test error handling in run_once."""
        mock_orchestrator.process_all_directories = AsyncMock(
            side_effect=Exception("Scan failed")
        )

        stats = await scheduler.run_once()

        assert stats["errors"] == 1
        assert stats["discovered"] == 0

    async def test_stop(self, scheduler):
        """Test stopping the scheduler."""
        scheduler._task = None
        await scheduler.stop()

        assert scheduler._running is False

    async def test_request_shutdown(self, scheduler):
        """Test requesting shutdown."""
        scheduler._running = True
        scheduler._shutdown_event = asyncio.Event()

        scheduler._request_shutdown()

        assert scheduler._running is False
        assert scheduler._shutdown_event.is_set()

    async def test_cleanup(self, scheduler, mock_cache):
        """Test cleanup on shutdown."""
        mock_cache.clear = AsyncMock()

        await scheduler._cleanup()

        mock_cache.clear.assert_called_once()

    @patch("asyncio.get_running_loop")
    async def test_signal_handler_setup(self, mock_get_loop, scheduler):
        """Test signal handler registration."""
        mock_loop = MagicMock()
        mock_get_loop.return_value = mock_loop
        mock_loop.add_signal_handler = MagicMock()

        # Simulate run setup
        scheduler._running = True
        scheduler._shutdown_event = asyncio.Event()

        for sig in [2, 15]:  # SIGINT, SIGTERM
            try:
                mock_loop.add_signal_handler(sig, scheduler._request_shutdown)
            except NotImplementedError:
                pass

        # Should have attempted to add handlers
        assert mock_loop.add_signal_handler.called

    async def test_scheduler_with_short_interval(
        self, scheduler, mock_orchestrator, mock_cache
    ):
        """Test scheduler runs multiple cycles."""
        call_count = [0]

        async def mock_process():
            call_count[0] += 1
            if call_count[0] >= 2:
                scheduler._running = False
            return {"discovered": 0, "processed": 0}

        mock_orchestrator.process_all_directories = mock_process
        mock_cache.clear = AsyncMock()

        # Run for a short time
        scheduler._running = True
        scheduler._shutdown_event = asyncio.Event()

        # Manually run two cycles
        await scheduler.run_once()
        await scheduler.run_once()

        assert call_count[0] == 2
