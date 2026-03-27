-- countyScansTitle.dbo.tblS3Image 131,017 Affected Records
-- Only the s3path contain alpha-numerics

WITH SplitData AS (
	SELECT
	    *,
		value AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM countyScansTitle.dbo.tblS3Image
	CROSS APPLY STRING_SPLIT(s3FilePath, '/')
	WHERE value NOT LIKE '%none%'
)
SELECT recordID, s3FilePath, pageCount, fileSizeBytes, _ModifiedDateTime, _ModifiedBy,
CASE	
	WHEN part like '1%'
	THEN REPLACE(
FROM SplitData
WHERE part_number = 5 
	AND (part LIKE '%1' OR part LIKE '%2' OR part LIKE '%3')
GROUP BY part;

-- Count Of S3 Paths
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

-- Create Temp Table
WITH SplitData AS (
	SELECT
	    recordID,
		value AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM countyScansTitle.dbo.tblS3Image
	CROSS APPLY STRING_SPLIT(s3FilePath, '/')
	WHERE value NOT LIKE '%none%'
)
SELECT recordID
INTO #LND7726  -- Local temp table
FROM SplitData
WHERE part_number = 5 
    AND (part LIKE '%1%' OR part LIKE '%2%' OR part LIKE '%3%');

-- Identify If issue still exists, Check new records have come in. It looks like this is fixed in countyScansTitle
SELECT MAX(_CreatedDateTime), CountyName
FROM [countyScansTitle].[dbo].[tblS3Image] i
JOIN [countyScansTitle].dbo.tblRecord tr ON tr.recordId = i.recordID
JOIN [countyScansTitle].dbo.tbllookupCounties c ON tr.countyID = c.CountyID
WHERE tr.recordID IN (
SELECT *
FROM #LND7726
)
GROUP BY CountyName


-- Processing Table Query
SELECT DIMLCountyName, CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE countyName like '%1' OR countyName like '%2' OR countyName like '%3'
GROUP BY DIMLCountyName, CountyName
order by CountyName


SELECT *
FROM countyScansTitle.dbo.tblS3Image
WHERE recordID = '0022839c-8b5c-4820-b04d-9f110b29dbbf'


-- Create CST Processing Table
WITH SplitData AS (
	SELECT
	    recordID,
		s3FilePath,
		value AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM countyScansTitle.dbo.tblS3Image
	CROSS APPLY STRING_SPLIT(s3FilePath, '/')
	WHERE value NOT LIKE '%none%'
),
CountiesToFix AS (
	SELECT 
		recordID,
		s3FilePath,
		part AS original_county_name,
		CASE	
			WHEN part LIKE '%1' THEN LEFT(part, LEN(part) - 1)
			WHEN part LIKE '%2' THEN LEFT(part, LEN(part) - 1)
			WHEN part LIKE '%3' THEN LEFT(part, LEN(part) - 1)
			ELSE part
		END AS corrected_county_name
	FROM SplitData
	WHERE part_number = 5 
		AND (part LIKE '%1' OR part LIKE '%2' OR part LIKE '%3')
)
SELECT 
	recordID,
	s3FilePath AS old_s3FilePath,
	REPLACE(s3FilePath, '/' + original_county_name + '/', '/' + corrected_county_name + '/') AS new_s3FilePath,
	0 AS Processed
INTO countyScansTitle.dbo.tblS3Image_LND7726
FROM CountiesToFix

SELECT *
FROM countyScansTitle.dbo.tblS3Image_LND7726
WHERE Processed = -1

SELECT COUNT(*), Processed
FROM countyScansTitle.dbo.tblS3Image_LND7726
GROUP BY Processed

--UPDATE countyScansTitle.dbo.tblS3Image_LND7726
--SET Processed = 1
--WHERE recordID = '0099832f-642e-4b21-9fb3-98179bf9ab6e'

SELECT *
FROM countyScansTitle.dbo.tblS3Image
WHERE recordID = '0099832f-642e-4b21-9fb3-98179bf9ab6e'

--UPDATE countyScansTitle.dbo.tblS3Image
--SET s3FilePath = 's3://enverus-courthouse-prod-chd-plants/tx/shelby/0005/00054fd9-24a4-4e1c-8aba-a1684a0cb13a.pdf'
--WHERE recordID = '0009e17e-f7b5-4fa9-a6de-bea33e7665d3'


SELECT *
FROM countyScansTitle.dbo.tbldataloaderspercounty
ORDER BY LastProcessedDateLandLeaseProducer

SELECT DATETIME('2026-03-26T13:19:38') - DATETIME('2026-03-26T13:28:16')

SELECT TOP 1 CONCAT(storageFilePath, '\', originalFileName, fileType) as source_file_path, *
FROM countyScansTitle.dbo.tblrecord
WHERE recordid = '0009e17e-f7b5-4fa9-a6de-bea33e7665d3'


SELECT tr.recordID, CONCAT(storageFilePath, '\', originalFileName, fileType) as source_file_path
FROM countyScansTitle.dbo.tblrecord tr
LEFT JOIN countyScansTitle.dbo.tblS3Image_LND7726 s3 ON s3.recordID = tr.recordID
WHERE s3.Processed = -1

UPDATE countyScansTitle.dbo.tblS3Image WITH (ROWLOCK) SET s3FilePath = 's3://enverus-courthouse-prod-chd-plants/tx/reeves/0009/0009e17e-f7b5-4fa9-a6de-bea33e7665d3.pdf' WHERE recordID = '0009e17e-f7b5-4fa9-a6de-bea33e7665d3'

UPDATE countyScansTitle.dbo.tblS3Image_LND7726 WITH (ROWLOCK) SET Processed = 1 WHERE recordID = '0009e17e-f7b5-4fa9-a6de-bea33e7665d3'
