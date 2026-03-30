-- CS_Digital.dbo.tbllookupCounties Dev
-- Should we also update the storageFilePath?
SELECT DIMLCountyName, CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE countyName like '%1' OR countyName like '%2' OR countyName like '%3'
GROUP BY DIMLCountyName, CountyName
order by CountyName

SELECT COUNT(*), CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE s3FilePath like ('%/' + countyName + '/%')
and (countyName like '%1' OR countyName like '%2' OR countyName like '%3')
GROUP BY CountyName
ORDER BY COUNT(*) DESC, CountyName

-- Identify if issues still persists, looks like JOHNSON3 stoppped in 2017 the rest are still updating
SELECT MAX(_CreatedDateTime), CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE CountyName IN (
'JOHNSON1','WARD2','NEWTON1','PANOLA2','HARRISON2','TYLER1','RUSK2','JOHNSON3'
,'LOVING2','LASALLE2','KARNES2','CROCKETT2','JASPER1','THROCKMORTON2','SHELBY2'
)
GROUP BY CountyName

-- Gather recordIDs
SELECT _CreatedDateTime, r.recordID, CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE CountyName IN (
'JOHNSON1','WARD2','NEWTON1','PANOLA2','HARRISON2','TYLER1','RUSK2','JOHNSON3'
,'LOVING2','LASALLE2','KARNES2','CROCKETT2','JASPER1','THROCKMORTON2','SHELBY2'
)
GROUP BY _CreatedDateTime, r.recordID, CountyName
ORDER BY _CreatedDateTime DESC, CountyName DESC

SELECT *
FROM CS_DIgital.dbo.tbls3image
WHERE recordID = '0b48faa4-b451-4009-b7fd-7b81c68a3e0c'

SELECT *
FROM CS_Digital.dbo.tblrecord
WHERE recordID = '7659af16-93cc-47e9-970a-3a47e38235c6'

SET XACT_ABORT ON;
BEGIN TRAN;

SELECT TOP 10 s3FilePath
FROM CS_Digital.dbo.tblS3Image
WHERE s3FilePath LIKE '%JOHNSON1%'

UPDATE CS_Digital.dbo.tblS3Image
SET s3FilePath = REPLACE(s3FilePath, 'johnson1', 'johnson')
WHERE s3FilePath LIKE '%JOHNSON1%'

SELECT TOP 10 s3FilePath
FROM CS_Digital.dbo.tblS3Image
WHERE s3FilePath LIKE '%JOHNSON1%'

SELECT TOP 10 s3FilePath
FROM CS_Digital.dbo.tblS3Image
WHERE s3FilePath LIKE '%JOHNSON%'

ROLLBACK TRAN;
--COMMIT TRAN;

SELECT @@TRANCOUNT, XACT_STATE();

SELECT COUNT(*)
FROM CS_Digital.dbo.tblS3Image
WHERE s3FilePath LIKE '%enverus-courthouse-dev-chd-plants%'


-- Create CSD Processing Table
WITH SplitData AS (
	SELECT
	    recordID,
		s3FilePath,
		LTRIM(RTRIM(Split.part.value('.', 'NVARCHAR(MAX)'))) AS part,
		ROW_NUMBER() OVER (PARTITION BY recordID ORDER BY (SELECT NULL)) AS part_number
	FROM CS_Digital.dbo.tblS3Image
	CROSS APPLY (
		SELECT CAST('<x>' + REPLACE(s3FilePath, '/', '</x><x>') + '</x>' AS XML) AS xmldata
	) AS XMLData
	CROSS APPLY xmldata.nodes('/x') AS Split(part)
	WHERE LTRIM(RTRIM(Split.part.value('.', 'NVARCHAR(MAX)'))) NOT LIKE '%none%'
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
	0 as Processed
INTO CS_Digital.dbo.tblS3Image_LND7726
FROM CountiesToFix;

SELECT *
FROM CS_Digital.dbo.tblS3Image_LND7726
WHERE Processed != 0

SELECT *
FROM CS_Digital.dbo.tblS3Image_LND7726
WHERE recordID = 'a32f23d9-ccad-4907-b7fd-4c0215d17051'

SELECT *
FROM CS_Digital.dbo.tblS3Image_LND7726
WHERE recordID = 'a2f0c06f-d14f-4075-a815-8d89a4c086e8'

SELECT *
FROM CS_Digital.dbo.tblS3Image
WHERE recordID = 'a2f0c06f-d14f-4075-a815-8d89a4c086e8'

SELECT *
FROM CS_Digital.dbo.tblS3Image
WHERE recordID IN (SELECT recordID FROM CS_Digital.dbo.tblS3Image_LND7726 WHERE Processed =1)

SELECT COUNT(*), Processed
FROM CS_Digital.dbo.tblS3Image_LND7726 WITH (NOLOCK)
GROUP BY Processed

SELECT TOP 10 *
FROM CS_Digital.dbo.tblS3Image_LND7726
WHERE Processed = -1

SELECT TOP 1 *
FROM CS_Digital.dbo.tblrecord

SELECT tr.recordID, CONCAT(storageFilePath, '\\', tr.recordID, '.pdf') as source_file_path, new_s3FilePath as new_s3filepath
                                             FROM CS_Digital.dbo.tblrecord tr
                                             LEFT JOIN CS_Digital.dbo.tblS3Image_LND7726 s3 ON s3.recordID = tr.recordID
                                             WHERE s3.Processed = -1