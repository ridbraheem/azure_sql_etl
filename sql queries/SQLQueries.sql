------------------------------------------------------------------------------------------------------------ Stored Procedure

DROP table #Latestprices
DROP table #PreviouWeekprices
DROP table #PreviouMonthprices
DROP table #Changeprices


----------------------------------------Latest price

SELECT yh.Date as LatestDate,yh.DayOpen, yh.DayClose, yh.companyid
INTO #Latestprices
 FROM dbo.yahooDailychange yh
 INNER JOIN (SELECT y.companyid, MAX(y.Date) as maxdate 
			 FROM dbo.yahooDailychange y GROUP BY y.companyid) l ON yh.companyid = l.companyid AND l.maxdate = yh.Date

------------------------------------------7 day
SELECT yh.Date as LatestDate, yh.DayOpen, yh.DayClose, yh.companyid
INTO #PreviouWeekprices
 FROM dbo.yahooDailychange yh
 INNER JOIN (SELECT y.companyid, MAX(y.Date) as maxdate 
			 FROM dbo.yahooDailychange y JOIN dbo.yahooDailychange l ON y.companyid = l.companyid
			 AND DATEDIFF(day, y.Date, l.Date) = 7
			 GROUP BY y.companyid) ww ON ww.companyid = yh.companyid AND ww.maxdate= yh.Date

------------------------------------------30 day
SELECT yh.Date as LatestDate, yh.DayOpen, yh.DayClose, yh.companyid
INTO #PreviouMonthprices
 FROM dbo.yahooDailychange yh
 INNER JOIN (SELECT y.companyid, MAX(y.Date) as maxdate 
			 FROM dbo.yahooDailychange y JOIN dbo.yahooDailychange l ON y.companyid = l.companyid
			 AND DATEDIFF(day, y.Date, l.Date) = 30
			 GROUP BY y.companyid) ww ON ww.companyid = yh.companyid AND ww.maxdate= yh.Date

----------------------------- Price Changes

SELECT l.LatestDate, l.companyid, CAST(ROUND(l.DayClose,2) as numeric(36,2)) 'Current Value'
, CAST(ROUND(l.DayClose - w.DayOpen,2) as numeric(36,2))  '$ Week'
, CAST(ROUND((l.DayClose -  w.DayOpen) / w.DayOpen,2) as numeric(36,2)) '% Week'
, CAST(ROUND(l.DayClose - m.DayOpen,2) as numeric(36,2)) '$ 30 Day'
, CAST(ROUND((l.DayClose -  m.DayOpen) /m.DayOpen,2) as numeric(36,2)) '% 30 Day'
INTO #Changeprices
FROM dbo.#Latestprices l JOIN dbo.#PreviouWeekprices p ON l.companyid = p.companyid
						 JOIN dbo.#PreviouMonthprices m ON l.companyid = m.companyid
						 JOIN dbo.#PreviouWeekprices w ON l.companyid = w.companyid
ORDER by l.companyid ASC

---------------------------- JOIN  company and sector table

SELECT c.ShortName, c.symbol, s.sector, ch.* 
FROM dbo.#Changeprices ch JOIN dbo.companyProfile c ON c.companyid = ch.companyid
JOIN sector s on s.sectorid =  c.sectorid

----------------------------------------------------------------------------------------------------Other Queries

------------------------------------------ Query to get company and holder info

SELECT c.companyid, c.ShortName, c.symbol, h.Holderid, h.Holder, d.DateReported, d.Shares, d.Value
FROM dbo.companyProfile c JOIN dbo.holdingDetails d ON d.companyid = c.companyid
JOIN dbo.majorHolders h ON d.Holderid = h.Holderid

------------------------------------------ Query to get commpany and news info

SELECT c.ShortName, c.symbol, s.sector, n.* 
FROM dbo.nytapi n  JOIN dbo.companyProfile c ON c.companyid = n.companyid
JOIN sector s on s.sectorid =  c.sectorid

----------------------------------------- Query to get company and price info

SELECT c.ShortName, c.symbol, s.sector, y.* 
FROM dbo.yahooDailychange y JOIN dbo.companyProfile c ON c.companyid = y.companyid
JOIN sector s on s.sectorid =  c.sectorid









