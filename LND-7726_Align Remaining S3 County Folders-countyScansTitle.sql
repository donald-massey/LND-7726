-- countyScansTitle.dbo.tblS3Image 131,017 Affected Records
-- Only the s3path contain alpha-numerics

WITH SplitData AS (
	SELECT
	    recordID,
		value AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM countyScansTitle.dbo.tblS3Image
	CROSS APPLY STRING_SPLIT(s3FilePath, '/')
)
SELECT *
FROM SplitData
WHERE part_number = 5 
    AND (part LIKE '%1%' OR part LIKE '%2%' OR part LIKE '%3%');


WITH SplitData AS (
	SELECT
	    recordID,
		value AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM countyScansTitle.dbo.tblS3Image
	CROSS APPLY STRING_SPLIT(s3FilePath, '/')
)
SELECT COUNT(*), part
FROM SplitData
WHERE part_number = 5 
    AND (part LIKE '%1%' OR part LIKE '%2%' OR part LIKE '%3%')
GROUP BY part
HAVING COUNT(*) > 500
ORDER BY COUNT(*) DESC;
