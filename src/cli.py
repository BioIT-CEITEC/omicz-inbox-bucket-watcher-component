"""Click CLI for S3 Bucket Watcher Service."""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

import click

from . import __version__
from .cache import ProcessingCache
from .cleanup import CleanupManager
from .config import get_config
from .orchestrator import Orchestrator
from .reporter import ChecksumReporter
from .s3_client import S3Client
from .scanner import DirectoryScanner
from .scheduler import Scheduler
from .transfer import TransferManager
from .validator import ChecksumValidator


def setup_logging(log_level: str) -> None:
    """Configure logging.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    """
    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        level=getattr(logging, log_level.upper(), logging.INFO),
        stream=sys.stdout,
    )


@click.command()
@click.version_option(version=__version__, prog_name="s3-watcher")
@click.option(
    "--config",
    "-c",
    "config_path",
    type=click.Path(exists=True),
    help="Path to YAML configuration file",
)
@click.option(
    "--log-level",
    "-l",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Logging level",
)
@click.option(
    "--once",
    is_flag=True,
    help="Run a single scan cycle and exit (instead of continuous polling)",
)
def main(config_path: Optional[str], log_level: str, once: bool) -> None:
    """S3 Bucket Watcher Service.

    Monitors S3 inbox directories for checksum manifests, validates file integrity,
    and transfers valid data to storage bucket.
    """
    setup_logging(log_level)
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = get_config(config_path)
        logger.info("Configuration loaded")

        # Create S3 client
        s3_client = S3Client(
            region=config.s3.region,
            endpoint_url=config.s3.endpoint_url,
            use_ssl=config.s3.use_ssl,
            verify_ssl=config.s3.verify_ssl,
        )

        # Create components
        cache = ProcessingCache()

        scanner = DirectoryScanner(
            s3_client=s3_client,
            inbox_bucket=config.s3.inbox_bucket,
            checksum_bucket=config.s3.checksum_bucket,
            directories=config.watcher.directories,
            settling_period_seconds=config.watcher.settling_period_seconds,
        )

        validator = ChecksumValidator(
            s3_client=s3_client,
            inbox_bucket=config.s3.inbox_bucket,
        )

        transfer_manager = TransferManager(
            s3_client=s3_client,
            storage_bucket=config.s3.storage_bucket,
            max_concurrent_copies=config.transfer.max_concurrent_copies,
            verify_after_copy=config.transfer.verify_after_copy,
            storage_prefix=config.transfer.storage_prefix,
            inbox_prefix=config.transfer.inbox_prefix,
        )

        cleanup_manager = CleanupManager(
            s3_client=s3_client,
            inbox_bucket=config.s3.inbox_bucket,
        )

        reporter = ChecksumReporter(
            s3_client=s3_client,
            checksum_bucket=config.s3.checksum_bucket,
            checksum_prefix=config.transfer.checksum_prefix,
        )

        orchestrator = Orchestrator(
            scanner=scanner,
            validator=validator,
            transfer_manager=transfer_manager,
            cleanup_manager=cleanup_manager,
            reporter=reporter,
            cache=cache,
            inbox_bucket=config.s3.inbox_bucket,
            max_concurrent_directories=config.watcher.max_concurrent_directories,
        )

        if once:
            # Run single cycle
            logger.info("Running single scan cycle")
            stats = asyncio.run(orchestrator.process_all_directories())
            logger.info(f"Cycle complete: {stats}")
        else:
            # Run continuous scheduler
            scheduler = Scheduler(
                orchestrator=orchestrator,
                cache=cache,
                poll_interval_seconds=config.watcher.poll_interval_seconds,
            )
            logger.info("Starting scheduler")
            asyncio.run(scheduler.run())

    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
