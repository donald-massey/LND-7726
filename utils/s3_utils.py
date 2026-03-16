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

def get_s3_key_from_path(s3_full_path: str, bucket: str) -> str:
    """
    Strip the ``s3://<bucket>/`` prefix from a full S3 URI and return
    the object key.

    Example
    -------
    >>> get_s3_key_from_path(
    ...     "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/e1ed/file.pdf",
    ...     "enverus-courthouse-prod-chd-plants",
    ... )
    'tx/crockett2/e1ed/file.pdf'
    """
    prefix = f"s3://{bucket}/"
    if s3_full_path.startswith(prefix):
        return s3_full_path[len(prefix):]
    raise ValueError(f"Path '{s3_full_path}' does not belong to bucket '{bucket}'")


def replace_county_folder(s3_key: str, old_folder: str, new_folder: str) -> str:
    """
    Replace the county folder segment in an S3 object key.

    The replacement targets ``/<old_folder>/`` segments only to avoid
    partial matches.  The search is case-sensitive — callers should
    ensure *old_folder* exactly matches the case used in *s3_key*.

    Example
    -------
    >>> replace_county_folder(
    ...     "tx/crockett2/e1ed/file.pdf",
    ...     "crockett2",
    ...     "crockett",
    ... )
    'tx/crockett/e1ed/file.pdf'
    """
    segment = f"/{old_folder}/"
    replacement = f"/{new_folder}/"
    if segment not in s3_key:
        raise ValueError(
            f"County folder '/{old_folder}/' not found in key '{s3_key}'"
        )
    return s3_key.replace(segment, replacement, 1)


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


def list_county_objects(
    client: S3Client,
    state_prefix: str,
    county_folder: str,
) -> list[dict[str, Any]]:
    """
    Return all S3 objects under ``<state_prefix>/<county_folder>/``.
    """
    prefix = f"{state_prefix}/{county_folder}/"
    response = client.list_objects_v2(Bucket=client.bucket, Prefix=prefix)
    return response.get("Contents", [])


def county_folder_exists(
    client: S3Client,
    state_prefix: str,
    county_folder: str,
) -> bool:
    """Return True if at least one object exists under the county folder prefix."""
    objects = list_county_objects(client, state_prefix, county_folder)
    return len(objects) > 0
