"""Async S3 client wrapper using aiobotocore.

Provides a clean abstraction over S3 operations with pagination handling,
streaming support, and basic retry logic for transient errors.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator, List, Optional

import aiobotocore.session
from aiobotocore.client import AioBaseClient
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class S3Object:
    """Represents an S3 object with metadata."""

    key: str
    size: int
    etag: str
    last_modified: datetime


@dataclass
class ObjectMetadata:
    """S3 object metadata from HEAD request."""

    content_length: int
    etag: str
    last_modified: Optional[datetime] = None


class S3Client:
    """Async S3 client wrapper with retry logic and pagination handling."""

    MAX_RETRIES = 3
    RETRY_DELAY = 0.5  # seconds

    def __init__(
        self,
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
        use_ssl: bool = True,
        verify_ssl: bool = True,
    ):
        """Initialize S3 client.

        Args:
            region: AWS region
            endpoint_url: Custom S3 endpoint (for MinIO/localstack)
            use_ssl: Use SSL for connections
            verify_ssl: Verify SSL certificates
        """
        self.region = region
        self.endpoint_url = endpoint_url
        self.use_ssl = use_ssl
        self.verify_ssl = verify_ssl
        self._session = aiobotocore.session.get_session()
        self._client: Optional[AioBaseClient] = None

    async def _get_client(self) -> AioBaseClient:
        """Get or create the S3 client."""
        if self._client is None:
            self._client = await self._session.create_client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
                use_ssl=self.use_ssl,
                verify=self.verify_ssl if self.verify_ssl else False,
            ).__aenter__()
        return self._client

    async def close(self) -> None:
        """Close the S3 client connection."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None

    async def _retry_operation(self, operation, *args, **kwargs):
        """Execute an operation with retry logic for transient errors."""
        last_error = None
        for attempt in range(self.MAX_RETRIES):
            try:
                return await operation(*args, **kwargs)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                # Retry on throttling, timeout, or 5xx errors
                if error_code in ["Throttling", "RequestTimeout", "InternalError", "ServiceUnavailable"]:
                    last_error = e
                    logger.warning(f"S3 transient error (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    await asyncio.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    raise
            except Exception as e:
                last_error = e
                logger.warning(f"S3 error (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                await asyncio.sleep(self.RETRY_DELAY * (attempt + 1))

        raise last_error

    async def list_objects_v2(
        self,
        bucket: str,
        prefix: str,
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
    ) -> AsyncIterator[S3Object]:
        """List objects in a bucket with pagination.

        Args:
            bucket: Bucket name
            prefix: Key prefix to filter by
            delimiter: Optional delimiter for grouping (e.g., '/')
            max_keys: Maximum number of keys to return

        Yields:
            S3Object instances for each matching object
        """
        client = await self._get_client()
        continuation_token = None

        while True:
            kwargs: dict[str, Any] = {
                "Bucket": bucket,
                "Prefix": prefix,
            }
            if delimiter:
                kwargs["Delimiter"] = delimiter
            if max_keys:
                kwargs["MaxKeys"] = max_keys
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token

            response = await self._retry_operation(client.list_objects_v2, **kwargs)

            for obj in response.get("Contents", []):
                yield S3Object(
                    key=obj["Key"],
                    size=obj["Size"],
                    etag=obj["ETag"].strip('"'),
                    last_modified=obj["LastModified"],
                )

            if not response.get("IsTruncated", False):
                break

            continuation_token = response.get("NextContinuationToken")

    async def get_object(self, bucket: str, key: str) -> AsyncIterator[bytes]:
        """Stream an object's content.

        Args:
            bucket: Bucket name
            key: Object key

        Yields:
            Bytes chunks of the object content
        """
        client = await self._get_client()
        response = await self._retry_operation(client.get_object, Bucket=bucket, Key=key)

        async for chunk in response["Body"]:
            yield chunk

    async def get_object_bytes(self, bucket: str, key: str) -> bytes:
        """Get entire object content as bytes.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            Complete object content as bytes
        """
        chunks = []
        async for chunk in self.get_object(bucket, key):
            chunks.append(chunk)
        return b"".join(chunks)

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes | str,
        content_type: Optional[str] = None,
    ) -> None:
        """Put an object in S3.

        Args:
            bucket: Bucket name
            key: Object key
            body: Object content (bytes or string)
            content_type: Optional content type
        """
        client = await self._get_client()
        kwargs = {
            "Bucket": bucket,
            "Key": key,
            "Body": body.encode() if isinstance(body, str) else body,
        }
        if content_type:
            kwargs["ContentType"] = content_type

        await self._retry_operation(client.put_object, **kwargs)

    async def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> None:
        """Copy an object within or between buckets.

        Args:
            source_bucket: Source bucket name
            source_key: Source object key
            dest_bucket: Destination bucket name
            dest_key: Destination object key
        """
        client = await self._get_client()
        copy_source = {"Bucket": source_bucket, "Key": source_key}

        await self._retry_operation(
            client.copy_object,
            CopySource=copy_source,
            Bucket=dest_bucket,
            Key=dest_key,
        )

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete a single object.

        Args:
            bucket: Bucket name
            key: Object key
        """
        client = await self._get_client()
        await self._retry_operation(client.delete_object, Bucket=bucket, Key=key)

    async def delete_objects(self, bucket: str, keys: List[str]) -> None:
        """Delete multiple objects in a batch.

        Args:
            bucket: Bucket name
            keys: List of object keys to delete
        """
        if not keys:
            return

        client = await self._get_client()

        # S3 DeleteObjects supports max 1000 keys per request
        batch_size = 1000
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            objects = [{"Key": key} for key in batch]

            response = await self._retry_operation(
                client.delete_objects,
                Bucket=bucket,
                Delete={"Objects": objects, "Quiet": True},
            )

            if errors := response.get("Errors", []):
                for error in errors:
                    logger.error(f"Failed to delete {error['Key']}: {error['Message']}")

    async def head_object(self, bucket: str, key: str) -> ObjectMetadata:
        """Get object metadata without downloading content.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            ObjectMetadata instance

        Raises:
            ClientError: If object doesn't exist
        """
        client = await self._get_client()
        response = await self._retry_operation(client.head_object, Bucket=bucket, Key=key)

        return ObjectMetadata(
            content_length=response["ContentLength"],
            etag=response["ETag"].strip('"'),
            last_modified=response.get("LastModified"),
        )

    async def object_exists(self, bucket: str, key: str) -> bool:
        """Check if an object exists.

        Args:
            bucket: Bucket name
            key: Object key

        Returns:
            True if object exists, False otherwise
        """
        try:
            await self.head_object(bucket, key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise
