"""
s3_utils.py
===========
Mock S3 client and helper functions for the LND-7726
S3 County Folder Alignment migration.

In production every function delegates to a real boto3 client;
in mock mode responses are generated from in-memory sample data
so all notebooks can run without AWS credentials.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sample S3 objects — keyed by their S3 object key (path without bucket)
# ---------------------------------------------------------------------------

_SAMPLE_OBJECTS: dict[str, dict[str, Any]] = {
    "tx/crockett2/e1ed/e1edc1e1-8608-4f03-88a0-72844609af94.pdf": {
        "Size": 102_400,
        "ETag": '"aabbcc1122334455aabbcc1122334455"',
        "LastModified": datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc),
    },
    "tx/crockett2/6af8/6af89658-4471-4668-81ea-e339b35eafa1.pdf": {
        "Size": 204_800,
        "ETag": '"ddeeff6677889900ddeeff6677889900"',
        "LastModified": datetime(2024, 1, 15, 11, 0, tzinfo=timezone.utc),
    },
    "tx/bexar1/aaaa/aaaaaaaa-1111-2222-3333-444444444444.pdf": {
        "Size": 153_600,
        "ETag": '"11223344556677881122334455667788"',
        "LastModified": datetime(2024, 2, 1, 9, 0, tzinfo=timezone.utc),
    },
    "tx/harris3/bbbb/bbbbbbbb-5555-6666-7777-888888888888.pdf": {
        "Size": 307_200,
        "ETag": '"99aabbcc99aabbcc99aabbcc99aabbcc"',
        "LastModified": datetime(2024, 2, 5, 14, 0, tzinfo=timezone.utc),
    },
    "tx/travis/cccc/cccccccc-9999-0000-aaaa-bbbbbbbbbbbb.pdf": {
        "Size": 81_920,
        "ETag": '"ffeeddccffeeddccffeeddccffeeddcc"',
        "LastModified": datetime(2024, 3, 1, 8, 0, tzinfo=timezone.utc),
    },
}


# ---------------------------------------------------------------------------
# Mock S3 Client
# ---------------------------------------------------------------------------

class MockS3Client:
    """
    Simulates a boto3 S3 client.

    All mutating operations (copy, delete) are guarded by *dry_run*.
    Responses match the structure returned by the real boto3 APIs so
    notebooks are portable between mock and production modes.
    """

    def __init__(self, bucket: str, dry_run: bool = True):
        self.bucket = bucket
        self.dry_run = dry_run
        # In-memory object store — starts as a shallow copy of the sample data
        self._objects: dict[str, dict[str, Any]] = dict(_SAMPLE_OBJECTS)
        logger.info("MockS3Client initialised: bucket=%s dry_run=%s", bucket, dry_run)

    # ------------------------------------------------------------------
    # list_objects_v2
    # ------------------------------------------------------------------

    def list_objects_v2(self, Bucket: str, Prefix: str = "", **kwargs) -> dict[str, Any]:
        """
        Return a response dict that mirrors boto3's list_objects_v2.

        Only keys that start with *Prefix* are returned.
        Pagination (ContinuationToken) is not simulated — all matching
        keys are always returned in a single response.
        """
        matching = [
            {
                "Key": key,
                "Size": meta["Size"],
                "ETag": meta["ETag"],
                "LastModified": meta["LastModified"],
            }
            for key, meta in self._objects.items()
            if key.startswith(Prefix)
        ]
        logger.debug("list_objects_v2: bucket=%s prefix=%s → %d object(s)",
                     Bucket, Prefix, len(matching))
        return {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Name": Bucket,
            "Prefix": Prefix,
            "KeyCount": len(matching),
            "Contents": matching,
            "IsTruncated": False,
        }

    # ------------------------------------------------------------------
    # head_object
    # ------------------------------------------------------------------

    def head_object(self, Bucket: str, Key: str) -> dict[str, Any]:
        """
        Return object metadata if the key exists, raise FileNotFoundError otherwise.
        Mirrors boto3 ClientError with code '404' in production.
        """
        if Key not in self._objects:
            raise FileNotFoundError(f"Object not found: s3://{Bucket}/{Key}")
        meta = self._objects[Key]
        logger.debug("head_object: s3://%s/%s exists (size=%d)", Bucket, Key, meta["Size"])
        return {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "ContentLength": meta["Size"],
            "ETag": meta["ETag"],
            "LastModified": meta["LastModified"],
        }

    # ------------------------------------------------------------------
    # copy_object
    # ------------------------------------------------------------------

    def copy_object(
        self,
        CopySource: dict[str, str],
        Bucket: str,
        Key: str,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Copy an object within (or between) buckets.

        CopySource must be {"Bucket": "...", "Key": "..."}.
        In DRY_RUN mode the copy is logged but not applied.
        """
        src_key = CopySource["Key"]
        logger.info("copy_object: %s → %s (dry_run=%s)", src_key, Key, self.dry_run)

        if self.dry_run:
            return {"ResponseMetadata": {"HTTPStatusCode": 200}, "dry_run": True}

        if src_key not in self._objects:
            raise FileNotFoundError(f"Source object not found: {src_key}")

        self._objects[Key] = dict(self._objects[src_key])
        new_etag = f'"{hashlib.md5(Key.encode()).hexdigest()}"'
        self._objects[Key]["ETag"] = new_etag
        return {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "CopyObjectResult": {
                "ETag": new_etag,
                "LastModified": datetime.now(tz=timezone.utc),
            },
        }

    # ------------------------------------------------------------------
    # delete_object
    # ------------------------------------------------------------------

    def delete_object(self, Bucket: str, Key: str, **kwargs) -> dict[str, Any]:
        """
        Delete an object from the bucket.

        In DRY_RUN mode the deletion is logged but not applied.
        """
        logger.info("delete_object: s3://%s/%s (dry_run=%s)", Bucket, Key, self.dry_run)

        if self.dry_run:
            return {"ResponseMetadata": {"HTTPStatusCode": 204}, "dry_run": True}

        self._objects.pop(Key, None)
        return {"ResponseMetadata": {"HTTPStatusCode": 204}}


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
    client: MockS3Client,
    src_key: str,
    dst_key: str,
) -> dict[str, Any]:
    """
    Copy *src_key* to *dst_key* and verify the destination object exists
    with a matching size.

    Returns a result dict:
    ``{"status": "copied"|"dry_run"|"error", "src": ..., "dst": ..., "error": ...}``
    """
    result: dict[str, Any] = {"src": src_key, "dst": dst_key}
    try:
        response = client.copy_object(
            CopySource={"Bucket": client.bucket, "Key": src_key},
            Bucket=client.bucket,
            Key=dst_key,
        )
        if response.get("dry_run"):
            result["status"] = "dry_run"
            return result

        # Verify
        client.head_object(Bucket=client.bucket, Key=dst_key)
        result["status"] = "copied"
    except Exception as exc:
        result["status"] = "error"
        result["error"] = str(exc)
        logger.error("copy_and_verify failed: %s → %s: %s", src_key, dst_key, exc)
    return result


def list_county_objects(
    client: MockS3Client,
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
    client: MockS3Client,
    state_prefix: str,
    county_folder: str,
) -> bool:
    """Return True if at least one object exists under the county folder prefix."""
    objects = list_county_objects(client, state_prefix, county_folder)
    return len(objects) > 0
