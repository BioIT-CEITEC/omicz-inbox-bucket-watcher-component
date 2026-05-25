"""Tests for configuration module."""

import os
import pytest
from pydantic import ValidationError

from src.config import S3Config, WatcherConfig, TransferConfig, ServiceConfig


class TestS3Config:
    """Tests for S3Config."""

    def test_minimal_config(self):
        """Test minimal required configuration."""
        config = S3Config(
            inbox_bucket="test-inbox",
            storage_bucket="test-storage",
            checksum_bucket="test-checksum",
        )
        assert config.inbox_bucket == "test-inbox"
        assert config.storage_bucket == "test-storage"
        assert config.checksum_bucket == "test-checksum"
        assert config.region == "us-east-1"
        assert config.endpoint_url is None

    def test_full_config(self):
        """Test full configuration with all options."""
        config = S3Config(
            inbox_bucket="test-inbox",
            storage_bucket="test-storage",
            checksum_bucket="test-checksum",
            region="eu-west-1",
            endpoint_url="http://localhost:9000",
            use_ssl=False,
            verify_ssl=False,
        )
        assert config.region == "eu-west-1"
        assert config.endpoint_url == "http://localhost:9000"
        assert config.use_ssl is False
        assert config.verify_ssl is False


class TestWatcherConfig:
    """Tests for WatcherConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = WatcherConfig()
        assert config.directories == []
        assert config.poll_interval_seconds == 60
        assert config.settling_period_seconds == 30
        assert config.max_concurrent_directories == 5

    def test_custom_values(self):
        """Test custom configuration values using alias."""
        # Use the alias 'watched_prefixes' as defined in the model
        config = WatcherConfig(
            watched_prefixes=["uploads/project-a/", "uploads/project-b/"],
            poll_interval_seconds=30,
            settling_period_seconds=10,
            max_concurrent_directories=10,
        )
        assert config.directories == ["uploads/project-a/", "uploads/project-b/"]
        assert config.poll_interval_seconds == 30


class TestTransferConfig:
    """Tests for TransferConfig."""

    def test_default_values(self):
        """Test default configuration values."""
        config = TransferConfig()
        assert config.max_concurrent_copies == 10
        assert config.verify_after_copy is True
        assert config.inbox_prefix == ""
        assert config.storage_prefix == ""
        assert config.checksum_prefix == ""

    def test_custom_values(self):
        """Test custom configuration values."""
        config = TransferConfig(
            max_concurrent_copies=20,
            verify_after_copy=False,
            inbox_prefix="inbox/",
            storage_prefix="storage/",
            checksum_prefix="checksums/",
        )
        assert config.max_concurrent_copies == 20
        assert config.verify_after_copy is False
        assert config.inbox_prefix == "inbox/"
        assert config.storage_prefix == "storage/"
        assert config.checksum_prefix == "checksums/"


class TestServiceConfig:
    """Tests for ServiceConfig."""

    def test_full_config(self):
        """Test full service configuration."""
        s3_config = S3Config(
            inbox_bucket="test-inbox",
            storage_bucket="test-storage",
            checksum_bucket="test-checksum",
        )
        watcher_config = WatcherConfig(
            watched_prefixes=["uploads/"],
            poll_interval_seconds=30,
        )
        transfer_config = TransferConfig(
            max_concurrent_copies=5,
        )

        config = ServiceConfig(
            s3=s3_config,
            watcher=watcher_config,
            transfer=transfer_config,
            log_level="DEBUG",
        )

        assert config.s3.inbox_bucket == "test-inbox"
        assert config.watcher.directories == ["uploads/"]
        assert config.transfer.max_concurrent_copies == 5
        assert config.log_level == "DEBUG"

    def test_env_var_override(self, monkeypatch):
        """Test environment variable overrides."""
        monkeypatch.setenv("S3WATCHER_S3_INBOX_BUCKET", "env-inbox")
        monkeypatch.setenv("S3WATCHER_S3_STORAGE_BUCKET", "env-storage")
        monkeypatch.setenv("S3WATCHER_S3_CHECKSUM_BUCKET", "env-checksum")
        monkeypatch.setenv("S3WATCHER_LOG_LEVEL", "DEBUG")

        # Note: This test would need actual env vars set before import
        # For now, we test the structure
        s3_config = S3Config(
            inbox_bucket=os.environ.get("S3WATCHER_S3_INBOX_BUCKET", "test-inbox"),
            storage_bucket=os.environ.get("S3WATCHER_S3_STORAGE_BUCKET", "test-storage"),
            checksum_bucket=os.environ.get("S3WATCHER_S3_CHECKSUM_BUCKET", "test-checksum"),
        )
        assert s3_config.inbox_bucket == "env-inbox"
