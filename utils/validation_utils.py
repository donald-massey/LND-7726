"""
validation_utils.py
===================
County name and S3 path validation helpers for LND-7726.

Rules (from the Jira ticket):
  ✅  Letters (upper or lower case)
  ✅  Spaces
  ✅  Periods
  ❌  Digits (0-9)
  ❌  All other special characters (underscore, hyphen, @, #, !, etc.)
"""

import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Validation patterns
# ---------------------------------------------------------------------------

# Matches a valid S3Key: only letters, spaces, and periods.
_VALID_S3KEY_PATTERN = re.compile(r"^[A-Za-z\s.]+$")

# Matches a trailing digit (one or more) at the end of a string.
_TRAILING_DIGITS_PATTERN = re.compile(r"\d+$")

# Matches any character that is NOT a letter, space, or period.
_INVALID_CHAR_PATTERN = re.compile(r"[^A-Za-z\s.]")

# Matches the county folder segment in a full S3 URI.
# Assumes the form:  s3://<bucket>/<state>/<county_folder>/...
_COUNTY_FOLDER_PATTERN = re.compile(
    r"s3://[^/]+/[^/]+/(?P<county_folder>[^/]+)/",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# S3Key validation
# ---------------------------------------------------------------------------

def is_valid_s3_key(s3_key: Optional[str]) -> bool:
    """
    Return True if *s3_key* conforms to the naming standard.

    A valid S3Key:
    - Is not None or empty.
    - Contains only letters, spaces, and periods.
    - Has no trailing digits.
    """
    if not s3_key or not s3_key.strip():
        return False
    return bool(_VALID_S3KEY_PATTERN.match(s3_key))


def has_trailing_digits(value: str) -> bool:
    """Return True if *value* ends with one or more digits."""
    return bool(_TRAILING_DIGITS_PATTERN.search(value))


def has_invalid_characters(value: str) -> bool:
    """
    Return True if *value* contains characters that are not allowed
    in an S3Key (digits, underscores, hyphens, etc.).
    """
    return bool(_INVALID_CHAR_PATTERN.search(value))


def validate_lookup_counties(rows: list[dict]) -> dict:
    """
    Validate a list of tblLookupCounties rows and return a report.

    Parameters
    ----------
    rows : list of dicts with at least {"CountyID", "CountyName", "S3Key"}

    Returns
    -------
    dict with keys:
      "valid"   : list of rows that pass all checks
      "invalid" : list of dicts describing failures
      "summary" : human-readable summary string
    """
    valid = []
    invalid = []

    for row in rows:
        s3_key = row.get("S3Key", "")
        issues = []

        if not s3_key or not str(s3_key).strip():
            issues.append("S3Key is NULL or empty")
        else:
            if has_trailing_digits(s3_key):
                issues.append(f"S3Key has trailing digits: '{s3_key}'")
            if has_invalid_characters(s3_key):
                issues.append(f"S3Key has invalid characters: '{s3_key}'")

        if issues:
            invalid.append({**row, "issues": issues})
            logger.warning("CountyID=%s S3Key='%s' — %s",
                           row.get("CountyID"), s3_key, "; ".join(issues))
        else:
            valid.append(row)

    summary = (
        f"Validated {len(rows)} counties: "
        f"{len(valid)} valid, {len(invalid)} need correction."
    )
    logger.info(summary)
    return {"valid": valid, "invalid": invalid, "summary": summary}


# ---------------------------------------------------------------------------
# S3Key correction
# ---------------------------------------------------------------------------

def correct_s3_key(s3_key: str) -> str:
    """
    Return a corrected version of *s3_key* that conforms to the naming standard.

    Transformations applied (in order):
    1. Strip leading / trailing whitespace.
    2. Remove trailing digits.
    3. Remove any remaining invalid characters (keep letters, spaces, periods).
    4. Normalise to lowercase (S3 paths are case-sensitive but the standard
       uses lowercase folder names).
    5. Strip any whitespace that was exposed by removals.
    """
    corrected = s3_key.strip()

    # Replace underscores and hyphens with spaces before stripping invalid chars
    corrected = corrected.replace("_", " ").replace("-", " ")

    # Remove trailing digits
    corrected = _TRAILING_DIGITS_PATTERN.sub("", corrected)

    # Remove invalid characters (preserve letters, spaces, periods)
    corrected = _INVALID_CHAR_PATTERN.sub("", corrected)

    # Normalise to lowercase
    corrected = corrected.lower()

    # Remove leading/trailing whitespace that may have been exposed
    corrected = corrected.strip()

    if corrected != s3_key:
        logger.info("S3Key corrected: '%s' → '%s'", s3_key, corrected)

    return corrected


def generate_correction_map(invalid_rows: list[dict]) -> list[dict]:
    """
    Build a list of correction records from the *invalid* rows returned
    by :func:`validate_lookup_counties`.

    Each record contains:
      county_id, county_name, old_s3_key, new_s3_key, changes_needed (bool)
    """
    corrections = []
    for row in invalid_rows:
        old_key = row.get("S3Key", "") or ""
        new_key = correct_s3_key(old_key)
        entry = {
            "county_id":       row.get("CountyID"),
            "county_name":     row.get("CountyName"),
            "old_s3_key":      old_key,
            "new_s3_key":      new_key,
            "changes_needed":  old_key != new_key,
        }
        # Carry through database_name if present (added by notebook 2 before calling this)
        if "database_name" in row:
            entry["database_name"] = row["database_name"]
        corrections.append(entry)
    return corrections


# ---------------------------------------------------------------------------
# Path standardisation
# ---------------------------------------------------------------------------

def extract_county_folder(s3_full_path: str) -> Optional[str]:
    """
    Extract the county folder segment from a full S3 URI.

    Example
    -------
    >>> extract_county_folder(
    ...     "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/e1ed/file.pdf"
    ... )
    'crockett2'
    """
    match = _COUNTY_FOLDER_PATTERN.search(s3_full_path)
    if match:
        return match.group("county_folder")
    logger.warning("Could not extract county folder from path: %s", s3_full_path)
    return None


def standardise_path(s3_full_path: str, old_folder: str, new_folder: str) -> str:
    """
    Replace *old_folder* with *new_folder* in a full S3 URI.

    Only the first occurrence of ``/<old_folder>/`` is replaced to
    prevent accidental substitution in subdirectory names.

    Example
    -------
    >>> standardise_path(
    ...     "s3://enverus-courthouse-prod-chd-plants/tx/crockett2/e1ed/file.pdf",
    ...     "crockett2",
    ...     "crockett",
    ... )
    's3://enverus-courthouse-prod-chd-plants/tx/crockett/e1ed/file.pdf'
    """
    old_segment = f"/{old_folder}/"
    new_segment = f"/{new_folder}/"
    if old_segment not in s3_full_path:
        raise ValueError(
            f"Segment '/{old_folder}/' not found in path: {s3_full_path}"
        )
    return s3_full_path.replace(old_segment, new_segment, 1)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def reconcile_paths(
    db_rows: list[dict],
    s3_keys_present: set[str],
    bucket: str,
) -> dict:
    """
    Compare database records against known S3 object keys and report discrepancies.

    Parameters
    ----------
    db_rows          : list of tblS3Image rows {"recordID", "s3FilePath"}
    s3_keys_present  : set of S3 object keys that actually exist
    bucket           : S3 bucket name (used to strip the URI prefix)

    Returns
    -------
    dict with keys:
      "matched"   : rows where the S3 object exists
      "missing"   : rows where the S3 object is absent
      "summary"   : human-readable summary string
    """
    prefix = f"s3://{bucket}/"
    matched = []
    missing = []

    for row in db_rows:
        path = row.get("s3FilePath", "")
        s3_key = path[len(prefix):] if path.startswith(prefix) else path

        if s3_key in s3_keys_present:
            matched.append(row)
        else:
            missing.append(row)
            logger.warning("Missing S3 object for recordID=%s path=%s",
                           row.get("recordID"), path)

    summary = (
        f"Reconciliation: {len(db_rows)} records checked — "
        f"{len(matched)} present ✅, {len(missing)} missing ❌"
    )
    logger.info(summary)
    return {"matched": matched, "missing": missing, "summary": summary}
