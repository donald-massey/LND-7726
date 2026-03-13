-- CS_Digital.dbo.tbllookupCounties Dev
SELECT count(*), DIMLCountyName, CountyName
FROM [CS_Digital].[dbo].[tblS3Image] i
JOIN [CS_Digital].dbo.tblRecord r ON r.recordId = i.recordID
JOIN [CS_Digital].dbo.tbllookupCounties c ON r.countyID = c.CountyID
WHERE s3FilePath like ('%/' + countyName + '/%')
and (countyName like '%1' OR countyName like '%2' OR countyName like '%3')
GROUP BY DIMLCountyName, CountyName
order by COUNT(*) DESC, CountyName