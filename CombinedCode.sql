
--Create Master Database for this work
create database CRIMEDATA 
--------------------------------------------------------------------------

USE [CRIMEDATA]
go

---------------------------------------------------------------------------
/* 
Shows tables and columns with data types: 
*/

/*
--Example table for Crime data showing raw data columns and data types:
[dbo].[RawCrime](
	[Crime ID] [varchar](100) NULL,
	[Month] [varchar](50) NULL,
	[Reported by] [varchar](100) NULL,
	[Falls within] [varchar](100) NULL,
	[Longitude] [varchar](50) NULL,
	[Latitude] [varchar](50) NULL, 
	[Location] [varchar](200) NULL,
	[LSOA code] [varchar](50) NULL,
	[LSOA name] [varchar](50) NULL,
	[Crime type] [varchar](200) NULL,
	[Last outcome category] [varchar](300) NULL,
	[Context] [varchar](500) NULL
) 
*/


---------------------------------------------------------------------------------------------------------
/* 
Schema Creations and transfers performed between cleaning data and starting to create model:
*/

create schema raww -- Schema to move all raw data into
go
create schema Final --Schema to create model tables in
go


ALTER SCHEMA raww 
    TRANSFER [dbo].[RawCrime2010txt]; --Contains raw data from 2010-2015
ALTER SCHEMA raww 
    TRANSFER [dbo].[RecentCrimetxt]; --Contains raw data from 2010-2015
ALTER SCHEMA raww 
    TRANSFER [dbo].[RawLSOA]; --Contains raw LSOA to Local Authority conversions
ALTER SCHEMA raww 
    TRANSFER [dbo].[RawLaRegion]; --Contains raw LA to Region conversions
ALTER SCHEMA raww 
    TRANSFER [dbo].[GovSpendPublicOrder]; --Contains spending estimates on public order and safety per region per year
ALTER SCHEMA raww 
    TRANSFER [dbo].[RegionalTobaccoShop]; --Contains Raw data showing tobacco shops open within regions
ALTER SCHEMA raww 
    TRANSFER [dbo].[DrugDeathsRaw]; --Data on Deaths due to drugs over year
ALTER SCHEMA raww 
    TRANSFER [dbo].[RegionalSmokers]; --Gives a breakdown of the % of smokers in each region in 2017
ALTER SCHEMA raww 
    TRANSFER [dbo].[SmokerPopPercentages]; --Has percentage of population smokers by year by gender
ALTER SCHEMA raww 
    TRANSFER [dbo].[VapeRaw]; --Contains ~400 Vape shops names, opening years and postcodes
go

---------------------------------------------------------------------------------------------------------------
/*
Provided raw data has been imported under the same names and moved into the raww schema as above, the following query 
can run in order from this point on to create all tables used in the model
*/

USE CRIMEDATA
go

---------------------------------------------------------------------------------------------------------------
/*
Error Prevention
*/

--If running this query start to finish, Fact tables must be dropped before reference and Dim tables can be dropped and recreated due to the foreign key links:
if object_id('Final.CrimeFactTable') is not null
	drop table Final.CrimeFactTable
go

if object_id('final.DrugCrimeFactTable') is not null
	drop table final.DrugCrimeFactTable
go
--(These have been provided again with the table creations to allow for selective recreation of the tables without rerunning the entire query)

---------------------------------------------------------------------------------------------------------------
/*
Data Cleansing:
*/

--Combine the two imports to create one crime table
if object_id('raww.UnionCrime') is not null
	drop table [raww].[UnionCrime]
go

select
	*
into [raww].[UnionCrime]
from [raww].[RecentCrimetxt]
union ALL
select
	*
from [raww].[RawCrime2010txt]  
go



--Cleans Crimes Data and adjusts crime types to be uniform across time
if object_id('dbo.CrimeAll') is not null
	drop table dbo.CrimeAll
go

select
	 Ltrim(Rtrim([Crime ID]))					[Crime ID]
	,Ltrim(Rtrim([Month]))						[Month]
	,Ltrim(Rtrim([Reported by]))				[Reported by]
	,Ltrim(Rtrim([Falls within]))				[Falls within]
	,Ltrim(Rtrim([Longitude]))					[Longitude]
	,Ltrim(Rtrim([Latitude]))					[Latitude]
	,Ltrim(Rtrim([Location]))					[Location]
	,Ltrim(Rtrim([LSOA code]))					[LSOA code]
	,Ltrim(Rtrim([LSOA name]))					[LSOA name]
	,case --Crime types are combined back into original categories to allow breakdown of crimetype counts over time
		when Ltrim(Rtrim([Crime type]))	= 'Violent crime' Then 'Violence and sexual offences'
		when Ltrim(Rtrim([Crime type])) in ('Bicycle theft', 'Theft from the person', 'Other theft') Then 'Theft'
		when Ltrim(Rtrim([Crime type])) in ('Possession of weapons', 'Public order', 'Public disorder and weapons') Then 'Public disorder and weapons'
		else Ltrim(Rtrim([Crime type]))					
	end	[Crime type]
	,Ltrim(Rtrim([Last outcome category]))		[Last outcome category]
	,Ltrim(Rtrim([Context]))					[Context]
into dbo.CrimeAll
from raww.UnionCrime



--Cleaning LSOA Data and choosing relevant columns
if object_id('dbo.CleanLsoa') is not null
	drop table dbo.CleanLsoa
go

select 
	RTRIM(LTRIM([PCD7])) as PostCode
	,RTRIM(LTRIM([LSOA11CD])) as LSOA11Code
	,RTRIM(LTRIM([LSOA11NM])) as LSOA11Name
	,RTRIM(LTRIM([LAD11CD])) as LocAuthDist11Code
	,RTRIM(LTRIM([LAD11NM])) as LocalAuthority
into [dbo].[CleanLsoa]
from [raww].[RawLSOA]



--Cleaning raw data allowing conversions from Local Authority to Region
if object_id('dbo.LARegion') is not null
	drop table dbo.LARegion
go

select
	 LTRIM(RTRIM([la_code])) [la_code]
      ,LTRIM(RTRIM([la_name])) [la_name]
      ,LTRIM(RTRIM([region_code])) [region_code]
      ,LTRIM(RTRIM([region_name])) [region_name]
into dbo.LARegion
from [raww].[RawLARegion]



--Cleans raw population estimates to only include LSOA codes and names with population not broken down by age
if object_id('dbo.PopEsti') is not null
	drop table dbo.PopEsti
go

select
	[Area Codes] LSOA_cd
	,LTRIM(RTRIM([F3])) Area_Nm
	,[All Ages] [Population]
into dbo.PopEsti
from raww.popLSOA
where [Area Names] is null


--Choosing relevant columns from drug misuse death data and cleaning for later merge 
if object_id('dbo.DrugDeaths') is not null
	drop table dbo.DrugDeaths
go

select 
	[F1] as [Yr]
	,LTRIM(RTRIM([F2])) as LocationCd
	,Case --Cleans regions to be comparable with other tables
		when LTRIM(RTRIM([F3])) = 'East of England' then 'East'
	Else LTRIM(RTRIM([F3]))
	End [Region]
	,[F4] as TotalDrugDeaths
	,[F5] as TotalAgeStandardizedMortalityRate
	,[F8] as DrugMisuseDeaths
	,[F9] as [MisuseASMR] 
into dbo.DrugDeaths
from [raww].[DrugDeathsRaw]


--Unpivotting data for tobacco shops per region and year while cleaning for later merge 
if object_id('dbo.RegionYrTobaccoShops') is not null
	drop table dbo.RegionYrTobaccoShops
go

select
	Region
	,Yr
	,TobaccoShopCount
into dbo.RegionYrTobaccoShops
from
(
select 
	LTRIM(RTRIM(region)) as [Region]
	,[2010] 
	,[2011] 
	,[2012] 
	,[2013] 
	,[2014]	
	,[2015]	
	,[2016]	
	,[2017]
	,[2018]	
from [raww].[RegionalTobaccoShop]
) p
unpivot --Instead of a column for each year, there is a row per region per year which allows easier joins with other tables.
(TobaccoShopCount for Yr in(	
	[2010] 
	,[2011] 
	,[2012] 
	,[2013] 
	,[2014]	
	,[2015]	
	,[2016]	
	,[2017]
	,[2018]	
)) as unpivtRegionalShops;
go



--Cleaning data and column names for regional spending on Public Order
if object_id('dbo.GovSpendRgnYr') is not null
	drop table dbo.GovSpendRgnYr
go

select --This unpivots the data while also cleaning
	Region
	,Yr
	,Spend
into dbo.GovSpendRgnYr
from
(
select 
	 LTRIM(RTRIM([Region])) [Region]
      ,LTRIM(RTRIM([2012-13_outturn])) as [2013]
      ,LTRIM(RTRIM([2013-14_outturn])) as [2014]
      ,LTRIM(RTRIM([2014-15_outturn])) as [2015]
      ,LTRIM(RTRIM([2015-16_outturn])) as [2016]
      ,LTRIM(RTRIM([2016-17_outturn])) as [2017]
from [raww].[GovSpendPublicOrder]
) p
unpivot --Instead of a column for each year, there is a row per region per year which allows easier joins with other tables.
(Spend for Yr in(	
	[2013] 
	,[2014]	
	,[2015]	
	,[2016]	
	,[2017]
)) as unpivtSpend;
go




-----------------------------------------------------------------------------------------------------------
/*
Create Reference Tables:
*/

--Reference Table for the Crime Type column
if object_id('Final.RefCrimeType') is not null
	drop table Final.RefCrimeType
go

select distinct
	isnull([Crime Type], 'Unknown') [CrimeType]
into Final.RefCrimeType
from CrimeAll
go

alter table Final.RefCrimeType
add CrimeTypeID tinyint identity primary key
go --16 rows



--Reference Table for the Outcome column
if object_id('Final.RefOutcome') is not null
	drop table Final.RefOutcome
go

select distinct
	isnull(nullif([Last outcome category],' '), 'Unknown') [Outcome]
into Final.RefOutcome
from CrimeAll

alter table Final.RefOutcome
add OutcomeID int identity primary key
go --27 rows



--Reference Table for the context column
if object_id('Final.RefContext') is not null
	drop table Final.RefContext
go

select distinct
	isnull(NullIf([context],' '), 'Unknown') [Context]
into Final.RefContext
from CrimeAll

alter table Final.RefContext
add ContextID int identity primary key
go -- 55 rows

--------------------------------------------------------------------------------------------
/*
Create Enrichment Dimension Tables:
*/

--Dimension table containing enrichment for the date
if object_id('Final.DateTable') is not null
	drop table Final.DateTable
go

select distinct -- Splits Month and Year Columns as sets as int
	[Month]
	,Cast(substring(LTRIM(RTRIM([Month])),1,4) as int) [Yr]
	,Cast(substring(LTRIM(RTRIM([Month])),6,2) as int) [Mnth]
	,case	 --Adds seasonal column
	when Cast(substring(LTRIM(RTRIM([Month])),6,2) as int) between 3 and 5 then 'Spring'
	when Cast(substring(LTRIM(RTRIM([Month])),6,2) as int) between 6 and 8 then 'Summer'
	when Cast(substring(LTRIM(RTRIM([Month])),6,2) as int) between 9 and 11 then 'Autumn'
	else 'Winter'
	end [Season]
	,Cast(LTRIM(RTRIM([Month])) + '-01' as date)[FullDate]
into Final.DateTable
from CrimeAll

alter table Final.DateTable
add DateID int identity primary key
go --92 rows

 

--Dimension table containing enrichment for the LSOA area
if object_id('Final.DimLocation') is not null
	drop table Final.DimLocation
go

select distinct
	l.LSOA11Code
	,l.LSOA11Name
	,l.LocalAuthority
	,l.LocAuthDist11Code
	,isnull(r.region_code, 'E12000006') [RegionCode] --Replaces missing null values
	,isnull(r.region_name, 'East') [RegionName]
into Final.DimLocation
from CleanLsoa l
left join dbo.LARegion r
	on l.LocAuthDist11Code = r.la_Code 
where LTRIM(RTRIM(LSOA11Code)) <> '' -- Exclude incomplete data

alter table Final.DimLocation 
add LocationID int identity primary key
go --34,753 rows



--Table which can be used to refer to constabulary ID and estimate regions for missing location data based on most common region the constabulary work in:
if object_id('final.constab') is not null
	drop table final.constab
go

with CTE as 
(
select distinct
	isnull([Reported by], 'Unknown') [Constab]
	,l.[regionname] 
	,l.RegionCode
	,count(*) over (partition by a.[Falls within], l.[regionname]) [Count]
from dbo.CrimeAll a 
left join [Final].[DimLocation] l
	on a.[LSOA code] = l.LSOA11Code
)
,CTE2 as
(
select distinct
	[Constab]
	,[RegionName] 
	,[RegionCode]
	,[count]
	,max([Count]) over (partition by [Constab]) [MaxCount]
from CTE
)
select distinct
	[Constab]
	,case
		when [Constab] not in ('Police Service of Northern Ireland','British Transport Police') then [regionname] 
	end [MostLikelyRegion]
	,case
		when [Constab] not in ('Police Service of Northern Ireland','British Transport Police') then [RegionCode]
	end [Most LikelyRegionCd]
into final.constab
from CTE2
where [count] = [MaxCount] 

alter table Final.Constab
add ConstabID tinyint identity primary key
go --45 rows



--Table containing the percentage of the population that are smokers for each year for each gender and age group. (Cleaned here and input straight into final.dim table)
if object_id('Final.PercentagePopSmokers') is not null
	drop table Final.PercentagePopSmokers
go

select
	LTRIM(RTRIM(IsNull([Year], 2011))) [Yr]
      ,[M-16-24]
      ,[M-25-34]
      ,[M-35-49]
      ,[M-50-59]
      ,[M-60 and over]
      ,[M-All aged 16 and over]
      ,[F-16-24]
      ,[F-25-34]
      ,[F-35-49]
      ,[F-50-59]
      ,[F-60 and over]
      ,[F-All aged 16 and over]
      ,[A-16-24]
      ,[A-25-34]
      ,[A-35-49]
      ,[A-50-59]
      ,[A-60 and over]
      ,[A-All aged 16 and over]
into Final.PercentagePopSmokers
from [raww].[SmokerPopPercentages]

alter table Final.PercentagePopSmokers
add SmokerPopID tinyint identity primary key
go 



--Info for analysis given by year and by region
if object_id('Final.DimAnalysisRgYr') is not null
	drop table Final.DimAnalysisRgYr
go

select
	d.* 
	,t.TobaccoShopCount
	,gs.Spend
into [Final].[DimAnalysisRgYr]
from dbo.DrugDeaths d
inner join dbo.RegionYrTobaccoShops t
	on d.Yr = t.yr and d.Region = t.Region
left join [dbo].[GovSpendRgnYr] gs --Left join will produce nulls as there is no 2011 or 2012 data here.
	on d.Yr = gs.yr and d.Region = gs.Region
where d.yr <> '2010' --Crime data does not contain full 2010 year data so this can be filtered out.

alter table Final.DimAnalysisRgYr
add AnalysisRgYrID tinyint identity primary key
go 

--------------------------------------------------------------------------------------
/*
Create fact table for all crime data
*/

if object_id('Final.CrimeFactTable') is not null
	drop table Final.CrimeFactTable
go

select 
	a.[Crime ID] --Note that this is the crimeID from the raw data in case it is later linked to the outcomes data provided in separate tables. It is null for many columns so is not a pk
	,[c1].[ConstabID] [Reported by] --Check revealed that [Reported by] and [Falls within] were always the same so only one column is included  
	,d.[DateID] 
	,a.[Longitude] 
	,a.[Latitude]
--	,a.Location --Not included in the fact table as it is not useful for my analysis, can be uncommented to include
	,l.[LocationID]
	,ct.[CrimeTypeID] 
	,o.[OutcomeID] 
	,x.[ContextID] 
into Final.CrimeFactTable
from CrimeAll a 
inner join [Final].[Constab] c1 
	on isnull(a.[Reported by], 'Unknown') = c1.[Constab] 
inner join [Final].[RefCrimeType] ct 
	on isnull(a.[Crime Type], 'Unknown') = ct.[CrimeType]
Left join [Final].[DimLocation] l
	on a.[LSOA code] = l.[LSOA11Code]
inner join [Final].[RefContext] x
	on isnull(nullIf(a.[context],' '), 'Unknown') = x.[Context]
inner join [Final].[RefOutcome] o
	on isnull(nullif(a.[Last outcome category],' '), 'Unknown') = o.[Outcome]
inner join [Final].[DateTable] d
	on a.[Month] = d.[month] 	
where a.[Falls within] <> 'Police Service of Northern Ireland' -- Crimes in Northern Ireland are filtered out here (About 1 million results)

alter table Final.CrimeFactTable
add TableID int identity primary key
go --45,672,561 rows

--Add Foreign Keys
alter table Final.CrimeFactTable
add constraint fk_ConstabID
foreign key ([Reported by]) REFERENCES [Final].[Constab](ConstabID);

alter table Final.CrimeFactTable
add constraint fk_CrimeTypeID
foreign key (CrimeTypeID) REFERENCES [Final].[RefCrimeType](CrimeTypeID);

alter table Final.CrimeFactTable
add constraint fk_LocationID
foreign key (LocationID) REFERENCES [Final].[DimLocation](LocationID);

alter table Final.CrimeFactTable
add constraint fk_ContextID
foreign key (ContextID) REFERENCES [Final].[RefContext](ContextID);

alter table Final.CrimeFactTable
add constraint fk_OutcomeID
foreign key (OutcomeID) REFERENCES [Final].[RefOutcome](OutcomeID);

alter table Final.CrimeFactTable
add constraint fk_DateID
foreign key (DateID) REFERENCES [Final].[DateTable](DateID);


--------------------------------------------------------------------------------------
/*
Filter main FactTable into only drug crimes while combining with smoking regional data to create new fact table:
*/

-- Create table containing only Drug Crimes
if object_id('final.DrugCrimeFactTable') is not null
	drop table final.DrugCrimeFactTable
go

select
	f.*
	,an.AnalysisRgYrID
	,p.SmokerPopID
into final.DrugCrimeFactTable 
from [final].[CrimeFactTable] f
inner join [final].[RefCrimeType] c
	on c.[CrimeTypeID] = f.[CrimeTypeID]
inner join [Final].[DateTable] d
	on f.[DateID] = d.[DateID] 	--This is needed to join Smoking data on 
inner join [final].[DimLocation] l
	on f.[locationID] = l.[locationID]
inner join [Final].[DimAnalysisRgYr] an
	on an.[Yr] = d.[Yr] and an.[Region] = l.[RegionName]
inner join [final].[PercentagePopSmokers] p
	on d.[Yr] = p.[Yr] --This table only contains years 2011 - 2017 so if set to 'inner join' it can filter out data from the incomplete years 2010 and 2018
where c.[CrimeType] = 'Drugs' --This filters the original table to include only the Drug crimes. (It is joined on CrimeType, not ID as the crimeTypeID for drugs may change when entire query is rerun)
go

alter table final.DrugCrimeFactTable
add constraint pk_DrugTableID primary key (TableID)
go --1,022,694

--Add Foreign Keys
alter table final.DrugCrimeFactTable
add constraint fk_DrugConstabID
foreign key ([Reported by]) REFERENCES [Final].[Constab](ConstabID);

alter table final.DrugCrimeFactTable
add constraint fk_DrugCrimeTypeID
foreign key (CrimeTypeID) REFERENCES [Final].[RefCrimeType](CrimeTypeID);

alter table final.DrugCrimeFactTable
add constraint fk_DrugLocationID
foreign key (LocationID) REFERENCES [Final].[DimLocation](LocationID);

alter table final.DrugCrimeFactTable
add constraint fk_DrugContextID
foreign key (ContextID) REFERENCES [Final].[RefContext](ContextID);

alter table final.DrugCrimeFactTable
add constraint fk_DrugOutcomeID
foreign key (OutcomeID) REFERENCES [Final].[RefOutcome](OutcomeID);

alter table final.DrugCrimeFactTable
add constraint fk_DrugDateID
foreign key (DateID) REFERENCES [Final].[DateTable](DateID);

alter table final.DrugCrimeFactTable
add constraint fk_SmokerPopID
foreign key (SmokerPopID) REFERENCES [Final].[PercentagePopSmokers](SmokerPopID);

alter table final.DrugCrimeFactTable
add constraint fk_AnalysisRgYrID
foreign key (AnalysisRgYrID) REFERENCES [Final].[DimAnalysisRgYr](AnalysisRgYrID);

------------------------------------------------------------------------------------------ 
/*
Example of how to enrich a dimension without needing to recreate the fact table:
*/

--Adds population estimates to the DimLocation table using the cleaned General Population data
alter table [Final].[DimLocation] --Add in a new column to be filled with values from another table
add [Population] int default null; --This will not rerun unless Final.DimLocation does not have a Population column 

update final.DimLocation --This retains the Primary key and relationship with the fact tables
set final.DimLocation.[Population] = --Fill in the Population column with values
(select dbo.PopEsti.[Population]
from dbo.PopEsti 
where final.DimLocation.LSOA11Code = dbo.PopEsti.LSOA_cd ); --Choose population when the LSOA codes match

------------------------------------------------------------------------------------------------
/*
Create Views or Tables for analysis and visualisation in Tableau
*/

--Table containing crime count broken down by various factors which can be aggragated within Tableau
if object_id('final.AnalysisCrimeCounts') is not null
	drop table final.AnalysisCrimeCounts
go

with CTE
as (
select distinct
	RegionName
	,sum([population]) over (partition by RegionName) [Pop]
from final.DimLocation
), 
CTE2 as
(
select
	FullDate
	,RegionName
	,CrimeType	
	,count(*) [CrimeCount]
from final.CrimeFactTable f
inner join final.DateTable d
	on f.DateID = d.DateID
inner join final.DimLocation l
	on f.locationID = l.LocationID
inner join final.RefCrimeType t
	on f.CrimeTypeID = t.CrimeTypeID
group by CrimeType, FullDate, RegionName
)
select	
	Yr
	,Mnth
	,Season
	,a.*
	,Pop
into final.AnalysisCrimeCounts
from CTE2 a
inner join final.DateTable d
	on a.FullDate = d.FullDate
inner join CTE c
	on a.RegionName = c.RegionName



--View showing the drug crime outcomes per year and region
if object_id('vw_DrugCrimeOutcome') is not null
	drop view vw_DrugCrimeOutcome
go

create view vw_DrugCrimeOutcome
as
select 
	YEAR(FullDate) [Yr]
	,RegionName
	,Outcome
	,count(*) [Frequency]
from final.DrugCrimeFactTable f
inner join final.RefOutcome o
	on f.OutcomeID = o.OutcomeID
inner join final.DateTable d
	on f.DateID = d.DateID
inner join final.DimLocation l
	on f.LocationID = l.LocationID
group by YEAR(d.FullDate), l.RegionName, o.Outcome

go



--View showing smoking population percentages in a format that allows easy Gender and Year filtering in Tableau 
if object_id('vw_SmokersPop') is not null
	drop view vw_SmokersPop
go

Create view vw_SmokersPop as 
with CTEBoth as
(
select
	LTRIM(RTRIM(IsNull([Year], 2011))) [Yr]
	,'All' [Gender]
      ,[A-16-24]					as [16-24]
      ,[A-25-34]					as [25-34]
      ,[A-35-49]					as [35-49]
      ,[A-50-59]					as [50-59]
      ,[A-60 and over]				as [60 and over]
      ,[A-All aged 16 and over]		as [All aged 16 and over]
from raww.SmokerPopPercentages
),
CTEMale as
(
select
	LTRIM(RTRIM(IsNull([Year], 2011))) [Yr]
	,'Male' [Gender]
      ,[M-16-24]					as [16-24]
      ,[M-25-34]					as [25-34]
      ,[M-35-49]					as [35-49]
      ,[M-50-59]					as [50-59]
      ,[M-60 and over]				as [60 and over]
      ,[M-All aged 16 and over]		as [All aged 16 and over]
from raww.SmokerPopPercentages
)
, CTEFemale as
(
select
	LTRIM(RTRIM(IsNull([Year], 2011))) [Yr]
	,'Female' [Gender]
      ,[F-16-24]					as [16-24]
      ,[F-25-34]					as [25-34]
      ,[F-35-49]					as [35-49]
      ,[F-50-59]					as [50-59]
      ,[F-60 and over]				as [60 and over]
      ,[F-All aged 16 and over]		as [All aged 16 and over]
from raww.SmokerPopPercentages
)
,CTECombined as(
select * 
from CTEBoth
union all
select * from CTEMale
union all 
select * from CTEFemale
)
select 
	*
from CTECombined



--View Containing regional comparison data on vape shops 
if object_id('vw_VapeCrimeCompare') is not null
	drop view vw_VapeCrimeCompare
go

create view vw_VapeCrimeCompare as
with CTE1 as ( --This CTE Counts the Vape shops per region
select 
	region_name as RegionName
	,Count(*) ShopCount
from VapeClean v
left join [dbo].[CleanLsoa] l
	on LTRIM(RTRIM(v.PostCode)) = l.PostCode
left join LARegion r
	on l.LocAuthDist11Code = r.la_code
Group by region_name
)
, CTE2 as
(
select distinct --This CTE provides population per region for normalisation
	RegionName, 
	sum([Population]) Pop
from final.DimLocation 
group by RegionName
)
select --Final select shows criem count, vape shops and population for each region
	a.[RegionName]
	,Sum(a.[CrimeCount]) [CrimeCount]
	,c.ShopCount
	,b.Pop
from [Final].[AnalysisCrimeCounts] a
	left join CTE1 c
		on a.[RegionName] = c.RegionName
	left join CTE2 b
		on b.RegionName = a.RegionName
where crimetype = 'Drugs'
group by a.[RegionName], c.ShopCount, b.Pop

-----------------------------------------------------------------------------------------------------------






--inner join CTEDrugCrime	
--	on ------Need to create new ID or something?


----View to show relationship between the number of tobacco shops in each region the drug crime counts
--if object_id('vw_CrimeCountAndTobaccoShops') is not null
--	drop view vw_CrimeCountAndTobaccoShops
--go

--create view vw_CrimeCountAndTobaccoShops
--as 
--With CTE as (
--Select 
--	count(*) [CrimeCount]
--	,f.AnalysisRgYrID
--from [final].[DrugCrimeFactTable] f
--group by f.AnalysisRgYrID
--)
--Select 
--	r.Region
--	,r.Yr
--	,c.[CrimeCount]
--	,r.TobaccoShopCount
--from CTE c
--inner join [Final].[DimAnalysisRgYr] r
--	on c.[AnalysisRgYrID] = r.[AnalysisRgYrID]
--where r.Yr <> 2011
--go



----View showing drug regional drug deaths and drug misuse deaths against drug crime counts
--if object_id('vw_CrimeCountAndDrugDeaths') is not null
--	drop view vw_CrimeCountAndDrugDeaths
--go

--create view vw_CrimeCountAndDrugDeaths
--as 
--With CTE as (
--Select 
--	count(*) [CrimeCount]
--	,f.AnalysisRgYrID
--from [final].[DrugCrimeFactTable] f
--group by f.AnalysisRgYrID
--)
--Select 
--	r.Region
--	,r.Yr
--	,c.[CrimeCount]
--	,r.TotalDrugDeaths
--	,r.DrugMisuseDeaths
--from CTE c
--inner join [Final].[DimAnalysisRgYr] r
--	on c.[AnalysisRgYrID] = r.[AnalysisRgYrID]
--where r.Yr <> 2011
--go



----View showing % of population that smoke by age group and gender
--if object_id('vw_CrimeCountAndSmokerPop') is not null
--	drop view vw_CrimeCountAndSmokerPop
--go

--create view vw_CrimeCountAndSmokerPop
--as 
--With CTE as (
--Select 
--	count(*) [CrimeCount]
--	,f.[SmokerPopID]
--from [final].[DrugCrimeFactTable] f
--group by f.[SmokerPopID]
--)
--Select 
--	p.*
--	,c.[CrimeCount]
--from CTE c
--inner join [final].[PercentagePopSmokers] p
--	on c.[SmokerPopID] = p.[SmokerPopID]
--where p.Yr <> 2011
--go



----view showing crime count break downs by month and Local Authority at most granular level with enrichment data included
--if object_id('vw_MonthlyCrime') is not null
--	drop view vw_MonthlyCrime
--go

--create view vw_MonthlyCrime
--as
--select 
--	d.FullDate
--	,d.Season
--	,l.RegionName
--	,l.LocalAuthority
--	,count(*) [CrimeCount]
--from final.DrugCrimeFactTable f
--inner join final.DateTable d
--	on f.DateID = d.DateID
--inner join final.DimLocation l
--	on f.locationID = l.LocationID
--group by FullDate, Season, RegionName, LocalAuthority




----view showing crime count break downs by month and Local Authority at most granular level with enrichment data included for all crime types
--if object_id('vw_MonthlyCrimeALLRgn') is not null
--	drop view vw_MonthlyCrimeALLRgn
--go

--create view vw_MonthlyCrimeALLRgn
--as
--select 
--	t.CrimeType
--	,d.FullDate
--	,d.Season
--	,l.RegionName
--	,count(*) [CrimeCount]
--from final.CrimeFactTable f
--inner join final.DateTable d
--	on f.DateID = d.DateID
--inner join final.DimLocation l
--	on f.locationID = l.LocationID
--inner join final.RefCrimeType t
--	on f.CrimeTypeID = t.CrimeTypeID
--group by CrimeType, FullDate, Season, RegionName

--select * from vw_MonthlyCrimeALLRgn
----
--if object_id('vw_SmokersPop') is not null
--	drop view vw_SmokersPop
--go
