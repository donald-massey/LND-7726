Problem Statement
JIRA: LND-7726 — Align Remaining S3 County Folders

Some county-specific folders in S3 still have erroneous names (numbers appended, e.g., Autauga1, Baldwin2) that don't match the canonical S3Key in tblLookupCounties. We need to:

Validate tblLookupCounties.S3Key values are clean (no trailing numbers, no special characters)
Discover all mismatched county folders by comparing CountyName in tblS3Image paths against S3Key in tblLookupCounties — across all three databases
Move all S3 objects from erroneous county folders to the correct folder path (creating the destination prefix if it doesn't exist)
Update s3FilePath in tblS3Image across all three databases to reflect the new S3 locations
Don't include any mock functions
Verify every record in tblS3Image has a corresponding object in S3 after the move
Reference Query (from ticket)
SQL
SELECT count(*), DIMLCountyName, CountyName
  FROM [dbo].[tblS3Image] i
  JOIN tblRecord r ON r.recordId = i.recordID
  JOIN tbllookupCounties c ON r.countyID = c.CountyID
  WHERE s3FilePath like ('%/' + countyName + '/%')
  AND (countyName like '%1' OR countyName like '%2' OR countyName like '%3')
GROUP BY DIMLCountyName, CountyName
ORDER BY CountyName
Technical Requirements
Runtime: Databricks notebooks (Python/PySpark)
Storage: AWS S3 — use boto3 for per-file copy+delete (need row-level audit trail)
Databases: Three databases, each with tblS3Image, tblRecord, tblLookupCounties
Connection: Spark SQL via JDBC to query all three databases
Notebook Structure
Notebook 0 — 00_config.py

Databricks widgets for: S3 bucket name, S3 base prefix, JDBC connection strings for all 3 databases, DRY_RUN flag (default True)
boto3 client initialization
Common helper functions (logging, S3 path manipulation)
County path segment extraction regex: extract county name from s3FilePath between known delimiters
Notebook 1 — 01_validate_lookup_table.py

For each of the 2 databases, query tblLookupCounties and check that S3Key:
Has no trailing digits
Has no special characters (only letters, spaces, hyphens allowed)
Is not NULL or empty
Compare S3Key values across all 3 databases to ensure consistency
Output a report of any S3Key values that need manual correction
STOP execution if any S3Key values are invalid — these must be fixed before proceeding
Notebook 2 — 02_discover_mismatches.py

For each of the 3 databases, run the reference query (expanded):
SQL
SELECT i.s3FilePath, r.recordId, c.CountyID, c.CountyName, c.S3Key, c.DIMLCountyName
FROM tblS3Image i
JOIN tblRecord r ON r.recordId = i.recordID
JOIN tblLookupCounties c ON r.countyID = c.CountyID
WHERE s3FilePath LIKE '%/' + CountyName + '/%'
  AND CountyName != S3Key
Produce a unified DataFrame: database_name | s3FilePath | old_county_folder | new_county_folder (S3Key) | old_s3_path | new_s3_path | record_id
The new_s3_path is computed by replacing the old_county_folder path segment with S3Key
Save this mapping to a Delta table: county_folder_migration_map
Display summary: count of files per database, per county correction
Notebook 3 — 03_move_s3_objects.py

Read the county_folder_migration_map Delta table
Support DRY_RUN widget parameter (default True):
When True: only log intended moves, count objects, estimate time
When False: execute moves
For each unique old_s3_path → new_s3_path:
Use boto3.client('s3').copy_object() to copy to new key
Verify the copy succeeded (compare ETag/size)
Use boto3.client('s3').delete_object() to remove the old key
Log result to audit Delta table: county_migration_audit with columns: old_key | new_key | status | timestamp | file_size | error_message
Handle idempotency: if source doesn't exist but destination does, log as "already_moved" and skip
After all moves, check for any remaining objects in old county folders using list_objects_v2 with the old prefix
Use batch processing with configurable batch size to avoid timeout
Notebook 4 — 04_update_database_paths.py

Read the county_folder_migration_map Delta table
Support DRY_RUN widget parameter (default True)
For each of the 3 databases:
SQL
UPDATE tblS3Image
SET s3FilePath = REPLACE(s3FilePath, '/{old_county_folder}/', '/{new_county_folder}/')
WHERE s3FilePath LIKE '%/{old_county_folder}/%'
  AND recordID IN (SELECT recordId FROM tblRecord WHERE countyID = {county_id})
Log rows affected per database per county correction
In DRY_RUN mode: run SELECT COUNT(*) with the same WHERE clause instead
Notebook 5 — 05_verify_integrity.py

For each of the 3 databases, query all tblS3Image.s3FilePath records
For each path, use boto3.client('s3').head_object() to verify the file exists in S3
Report:
Total records checked per database
Count of records where S3 object exists ✅
Count of records where S3 object is missing ❌ (with details)
Verify no old county folder prefixes remain in S3 (list_objects_v2 on old prefixes)
Verify no tblS3Image paths still reference old county folder names
Safety & Rollback
county_folder_migration_map Delta table persisted before any changes
county_migration_audit Delta table logs every S3 operation
DRY_RUN mode on all destructive notebooks (03, 04)
Rollback approach: reverse the audit log (copy from new→old, update DB paths back)
All notebooks should be re-runnable (idempotent)