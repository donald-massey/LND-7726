-- CS_Digital.dbo.tbllookupCounties Dev
-- Should we also update the storageFilePath?
SELECT DIMLCountyName, CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE s3FilePath like ('%/' + countyName + '/%')
and (countyName like '%1' OR countyName like '%2' OR countyName like '%3')
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
