"""Orchestrator module for coordinating the full directory processing lifecycle."""

from __future__ import annotations

import asyncio
import logging
from typing import List

from .cache import ProcessingCache
from .cleanup import CleanupManager
from .models import Directory, ValidationResult
from .reporter import ChecksumReporter
from .scanner import DirectoryScanner
from .transfer import TransferManager
from .validator import ChecksumValidator

logger = logging.getLogger(__name__)


class Orchestrator:
    """Central coordinator for directory processing lifecycle.

    Coordinates the full lifecycle:
    1. Scan for eligible directories
    2. Validate checksums
    3. On success: transfer, cleanup, report success
    4. On failure: report failure
    """

    def __init__(
        self,
        scanner: DirectoryScanner,
        validator: ChecksumValidator,
        transfer_manager: TransferManager,
        cleanup_manager: CleanupManager,
        reporter: ChecksumReporter,
        cache: ProcessingCache,
        inbox_bucket: str,
        max_concurrent_directories: int = 5,
    ):
        """Initialize the orchestrator.

        Args:
            scanner: Directory scanner
            validator: Checksum validator
            transfer_manager: Transfer manager
            cleanup_manager: Cleanup manager
            reporter: Checksum reporter
            cache: Processing cache
            inbox_bucket: Inbox bucket name
            max_concurrent_directories: Max directories to process concurrently
        """
        self.scanner = scanner
        self.validator = validator
        self.transfer_manager = transfer_manager
        self.cleanup_manager = cleanup_manager
        self.reporter = reporter
        self.cache = cache
        self.inbox_bucket = inbox_bucket
        self.max_concurrent_directories = max_concurrent_directories
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent_directories)

    async def process_all_directories(self) -> dict:
        """Scan and process all eligible directories.

        Returns:
            Dictionary with processing statistics
        """
        stats = {
            "discovered": 0,
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "errors": 0,
            "skipped": 0,
        }

        # Scan for eligible directories
        try:
            directories = await self.scanner.scan()
            stats["discovered"] = len(directories)
        except Exception as e:
            logger.error(f"Failed to scan directories: {e}")
            return stats

        if not directories:
            logger.debug("No directories to process")
            return stats

        logger.info(f"Discovered {len(directories)} directories to process")

        # Process directories with concurrency limit
        tasks = [self._process_with_semaphore(directory) for directory in directories]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        for directory, result in zip(directories, results):
            if isinstance(result, Exception):
                logger.error(f"Error processing {directory.prefix}: {result}")
                stats["errors"] += 1
                await self.cache.mark_completed(directory.prefix)
            elif result == "skipped":
                stats["skipped"] += 1
            elif result is True:
                stats["processed"] += 1
                stats["succeeded"] += 1
            else:
                stats["processed"] += 1
                stats["failed"] += 1

        logger.info(
            f"Processing complete: {stats['succeeded']} succeeded, "
            f"{stats['failed']} failed, {stats['errors']} errors, "
            f"{stats['skipped']} skipped"
        )

        return stats

    async def _process_with_semaphore(self, directory: Directory) -> bool | str:
        """Process a directory with semaphore-based concurrency control."""
        async with self._semaphore:
            return await self.process_directory(directory)

    async def process_directory(self, directory: Directory) -> bool | str:
        """Execute the full lifecycle for a single directory.

        Args:
            directory: Directory to process

        Returns:
            True if succeeded, False if failed, "skipped" if already processing
        """
        prefix = directory.prefix

        # Check if already being processed
        if await self.cache.is_processing(prefix):
            logger.debug(f"Directory {prefix} is already being processed, skipping")
            return "skipped"

        # Mark as processing
        await self.cache.mark_processing(prefix)

        try:
            log_context = {
                "prefix": prefix,
                "checksum_id": directory.checksum_id,
            }
            logger.info("Processing directory", extra=log_context)

            # Step 1: Validate checksums
            validation_result = await self.validator.validate(directory)

            if not validation_result.is_valid:
                logger.warning(
                    f"Validation failed for {prefix}: "
                    f"{len(validation_result.corrupted_files)} corrupted files, "
                    f"{validation_result.missing_hashes} missing hashes "
                    f"{validation_result.extra_files} extra files"
                )

                # Report failure
                await self.reporter.report_failure(
                    directory,
                    validation_result.corrupted_files
                )

                return False

            logger.info(f"Validation passed for {prefix}")

            # Step 2: Transfer to storage bucket
            transfer_result = await self.transfer_manager.transfer(
                directory, self.inbox_bucket
            )

            if not transfer_result.success:
                logger.error(f"Transfer failed for {prefix}")
                # Report failure for transfer errors
                await self.reporter.report_failure(
                    directory,
                    []
                )
                return False

            # Step 3: Cleanup inbox
            cleanup_success = await self.cleanup_manager.cleanup(directory)

            if not cleanup_success:
                logger.warning(f"Cleanup partially failed for {prefix}")

            # Step 4: Report success
            await self.reporter.report_success(directory)

            logger.info("Directory processing completed successfully", extra=log_context)
            return True

        except Exception as e:
            logger.error(f"Unexpected error processing {prefix}: {e}")
            return False

        finally:
            # Always mark as completed (even on error)
            await self.cache.mark_completed(prefix)
