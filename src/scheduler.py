"""Scheduler module for fixed-interval polling loop."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Optional

from .cache import ProcessingCache
from .orchestrator import Orchestrator

logger = logging.getLogger(__name__)


class Scheduler:
    """Manages fixed-interval polling cycles.

    Runs the orchestrator at configurable intervals with graceful shutdown support.
    """

    def __init__(
        self,
        orchestrator: Orchestrator,
        cache: ProcessingCache,
        poll_interval_seconds: int = 60,
    ):
        """Initialize the scheduler.

        Args:
            orchestrator: Orchestrator instance
            cache: Processing cache for cleanup on shutdown
            poll_interval_seconds: Interval between scan cycles
        """
        self.orchestrator = orchestrator
        self.cache = cache
        self.poll_interval_seconds = poll_interval_seconds
        self._running = False
        self._shutdown_event: Optional[asyncio.Event] = None
        self._task: Optional[asyncio.Task] = None

    async def run(self) -> None:
        """Main loop: run orchestrator at fixed intervals until shutdown.

        Handles graceful shutdown on SIGTERM/SIGINT.
        """
        self._running = True
        self._shutdown_event = asyncio.Event()

        # Set up signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_shutdown)
            except NotImplementedError:
                # Signal handlers not supported on this platform (e.g., Windows)
                logger.warning(f"Signal handler for {sig} not available")

        logger.info(
            "Scheduler started",
            extra={"poll_interval_seconds": self.poll_interval_seconds},
        )

        try:
            while self._running:
                # Run a single cycle
                await self.run_once()

                # Wait for next interval or shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.poll_interval_seconds,
                    )
                    # Shutdown was requested
                    break
                except asyncio.TimeoutError:
                    # Continue to next cycle
                    pass

        finally:
            await self._cleanup()
            logger.info("Scheduler stopped")

    async def run_once(self) -> dict:
        """Execute a single scan-and-process cycle.

        Returns:
            Dictionary with processing statistics
        """
        logger.info("Starting scan cycle")

        try:
            stats = await self.orchestrator.process_all_directories()
            logger.info("Scan cycle complete", extra=stats)
            return stats
        except Exception as e:
            logger.error(f"Error in scan cycle: {e}")
            return {
                "discovered": 0,
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
                "errors": 1,
                "skipped": 0,
            }

    def _request_shutdown(self) -> None:
        """Request graceful shutdown."""
        logger.info("Shutdown requested")
        self._running = False
        if self._shutdown_event:
            self._shutdown_event.set()

    async def stop(self) -> None:
        """Stop the scheduler gracefully."""
        self._request_shutdown()
        if self._task:
            await self._task

    async def _cleanup(self) -> None:
        """Clean up resources on shutdown."""
        # Clear the cache to release any held entries
        await self.cache.clear()

        # Close S3 client if available
        if hasattr(self.orchestrator, 'scanner') and hasattr(
            self.orchestrator.scanner, 's3_client'
        ):
            await self.orchestrator.scanner.s3_client.close()


def create_scheduler(
    orchestrator: Orchestrator,
    cache: ProcessingCache,
    poll_interval_seconds: int = 60,
) -> Scheduler:
    """Factory function to create a scheduler instance.

    Args:
        orchestrator: Orchestrator instance
        cache: Processing cache
        poll_interval_seconds: Polling interval

    Returns:
        Scheduler instance
    """
    return Scheduler(
        orchestrator=orchestrator,
        cache=cache,
        poll_interval_seconds=poll_interval_seconds,
    )
