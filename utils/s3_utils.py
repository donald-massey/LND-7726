from __future__ import annotations  # must be the FIRST import in the file
"""
s3_utils.py
===========
Real S3 client wrapper and helper functions for the LND-7726
S3 County Folder Alignment migration.

All S3 operations delegate to a real boto3 client.
AWS credentials must be available via the standard boto3 credential chain
(environment variables, ~/.aws/credentials, IAM instance role, etc.).
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 Client — thin wrapper that holds the target bucket alongside the client
# ---------------------------------------------------------------------------

class S3Client:
    """
    Thin wrapper around a real boto3 S3 client that tracks the target bucket.

    Maintains the same method interface used throughout the notebooks so that
    the bucket name does not need to be threaded through every call site.

    Parameters
    ----------
    bucket : target S3 bucket name
    region : AWS region (default ``us-east-1``)
    """

    def __init__(self, bucket: str, region: str = "us-east-1"):
        import boto3  # noqa: PLC0415

        self.bucket = bucket
        self._client = boto3.client("s3", region_name=region)
        logger.info("S3Client initialised: bucket=%s region=%s", bucket, region)

    def list_objects_v2(self, **kwargs) -> dict[str, Any]:
        """Delegate to boto3 ``list_objects_v2``."""
        return self._client.list_objects_v2(**kwargs)

    def head_object(self, **kwargs) -> dict[str, Any]:
        """
        Delegate to boto3 ``head_object``.

        Converts a 404 ``ClientError`` to ``FileNotFoundError`` to maintain
        the contract expected by the notebook helper functions.
        """
        from botocore.exceptions import ClientError  # noqa: PLC0415

        try:
            return self._client.head_object(**kwargs)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
                key = kwargs.get("Key", "")
                bucket = kwargs.get("Bucket", self.bucket)
                raise FileNotFoundError(
                    f"Object not found: s3://{bucket}/{key}"
                ) from exc
            raise

    def copy_object(self, **kwargs) -> dict[str, Any]:
        """Delegate to boto3 ``copy_object``."""
        return self._client.copy_object(**kwargs)

    def delete_object(self, **kwargs) -> dict[str, Any]:
        """Delegate to boto3 ``delete_object``."""
        return self._client.delete_object(**kwargs)


# ---------------------------------------------------------------------------
# High-level helpers used by the notebooks
# ---------------------------------------------------------------------------

def copy_and_verify(
    client: S3Client,
    src_key: str,
    dst_key: str,
) -> dict[str, Any]:
    """
    Copy *src_key* to *dst_key* and verify the destination object exists
    with a matching size.

    Returns a result dict:
    ``{"status": "copied"|"error", "src": ..., "dst": ..., "error": ...}``

    Note: DRY_RUN logic is expected to be handled at the caller level —
    do not call this function when dry-run mode is active.
    """
    result: dict[str, Any] = {"src": src_key, "dst": dst_key}

    # Strip s3:// prefix and bucket name if accidentally included
    src_key = src_key.replace(f"s3://{client.bucket}/", "")
    dst_key = dst_key.replace(f"s3://{client.bucket}/", "")

    try:
        client.copy_object(
            CopySource={"Bucket": client.bucket, "Key": src_key},
            Bucket=client.bucket,
            Key=dst_key,
        )

        # Verify destination exists
        client.head_object(Bucket=client.bucket, Key=dst_key)
        result["status"] = "copied"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        logger.error("copy_and_verify failed: %s → %s: %s", src_key, dst_key, exc)
    return result
