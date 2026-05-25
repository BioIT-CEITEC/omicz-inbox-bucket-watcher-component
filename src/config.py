"""Configuration module for S3 Bucket Watcher Service.

Uses Pydantic Settings with YAML file support and environment variable overrides.
Environment variables take priority over file-based configuration.
"""

from __future__ import annotations

from typing import List, Optional

from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)


class S3Config(BaseSettings):
    """S3 bucket and connection configuration."""

    model_config = SettingsConfigDict(env_prefix="S3WATCHER_S3_")

    inbox_bucket: str = Field(..., description="Source S3 bucket for incoming data")
    storage_bucket: str = Field(..., description="Destination S3 bucket for validated data")
    checksum_bucket: str = Field(..., description="S3 bucket for success/failure markers")
    region: str = Field(default="us-east-1", description="AWS region")
    endpoint_url: Optional[str] = Field(default=None, description="S3 endpoint URL (for MinIO/localstack)")
    use_ssl: bool = Field(default=True, description="Use SSL for S3 connections")
    verify_ssl: bool = Field(default=True, description="Verify SSL certificates")


class WatcherConfig(BaseSettings):
    """Watcher polling and directory scanning configuration."""

    model_config = SettingsConfigDict(env_prefix="S3WATCHER_WATCHER_")

    directories: List[str] = Field(
        default_factory=list,
        alias="watched_prefixes",
        description="List of directory prefixes to watch in the inbox bucket"
    )
    poll_interval_seconds: int = Field(default=60, description="Interval between scan cycles")
    settling_period_seconds: int = Field(
        default=30,
        description="Delay after discovering checksum file to allow S3 consistency"
    )
    max_concurrent_directories: int = Field(default=5, description="Max directories to process concurrently")


class TransferConfig(BaseSettings):
    """Transfer manager configuration."""

    model_config = SettingsConfigDict(env_prefix="S3WATCHER_TRANSFER_")

    max_concurrent_copies: int = Field(default=10, description="Max concurrent S3 copy operations")
    verify_after_copy: bool = Field(default=True, description="Verify objects after copying")
    inbox_prefix: str = Field(default="", description="Prefix to strip from source keys before transferring to storage")
    storage_prefix: str = Field(default="", description="Prefix to prepend to destination keys in storage bucket")
    checksum_prefix: str = Field(default="", description="Prefix to prepend to checksum marker keys in checksum bucket")


class ServiceConfig(BaseSettings):
    """Main service configuration combining all sections."""

    model_config = SettingsConfigDict(
        env_prefix="S3WATCHER_",
        yaml_file="config.yaml",
        yaml_file_encoding="utf-8",
        extra="ignore",
    )

    s3: S3Config = Field(..., description="S3 configuration")
    watcher: WatcherConfig = Field(..., description="Watcher configuration")
    transfer: TransferConfig = Field(default_factory=TransferConfig, description="Transfer configuration")
    log_level: str = Field(default="INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR)")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Include YAML config file as a settings source.

        Priority order (first wins):
        1. Init settings (constructor kwargs)
        2. Environment variables
        3. YAML config file
        """
        return (
            init_settings,
            env_settings,
            YamlConfigSettingsSource(settings_cls),
        )

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "ServiceConfig":
        """Load configuration from YAML file with environment variable overrides.

        Args:
            config_path: Optional path to YAML config file. If not provided,
                         uses default 'config.yaml' from model_config.

        Returns:
            ServiceConfig instance with merged configuration.
        """
        if config_path:
            return cls(_yaml_file=config_path)  # type: ignore[call-arg]
        return cls()  # type: ignore[call-arg]


def get_config(config_path: Optional[str] = None) -> ServiceConfig:
    """Convenience function to load configuration.

    Args:
        config_path: Optional path to YAML config file.

    Returns:
        ServiceConfig instance.
    """
    return ServiceConfig.load(config_path)
