create database if not exists cmrmsft;
DROP TABLE IF EXISTS cmrmsft.emt_display_rawinput;
CREATE EXTERNAL TABLE cmrmsft.emt_display_rawinput
(
  Country				String,
  Day					String,
  Media_Buy_Key			String,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			String,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			String,
  Clicks				String,
  Media_Costs			String,
  Video_Fully_Played	String,
  Rich_Impressions		String,
  Rich_Clicks			String,
  Total_Conversions		String,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String
  )  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE LOCATION 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/process/display/FY2015/' TBLPROPERTIES("skip.header.line.count"="1");

alter table cmrmsft.emt_display_rawinput set SERDEPROPERTIES ("serialization.null.format" = "");


DROP TABLE IF EXISTS cmrmsft.emt_display_summerylog;
CREATE EXTERNAL TABLE cmrmsft.emt_display_summerylog
(
   DATE					Date,
   YEAR					String,
   TYPE      			String,
   COUNTRY				String,
   MONTH          		String,
   RECORD_COUNT         int,
   FILE_NAME   			String
)  
ROW FORMAT DELIMITED FIELDS TERMINATED BY ''
STORED AS TEXTFILE LOCATION 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/summery_log/FY2015/';


INSERT INTO TABLE cmrmsft.emt_display_summerylog
Select
DATE,
YEAR,
TYPE,
COUNTRY,
MONTH,
Sum(RECORD_COUNT) RECORD_COUNT,
FILE_NAME from
(Select 	
   TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) DATE,
Case when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=01 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
	 when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=02 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
	 when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=03 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
	 when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=04 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
	 when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=05 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
	 when MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))=06 then concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))-1,'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),3,2))
Else
	 concat(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),'-',SUBSTR(YEAR(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date))+1,3,2)) 
End YEAR,
   "DISPLAY" TYPE,
   COUNTRY COUNTRY,
   MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)) MONTH,
   count(*) RECORD_COUNT,
   INPUT__FILE__NAME FILE_NAME   
From cmrmsft.emt_display_rawinput
group by 
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())),
DAY,
COUNTRY,
MONTH(cast(from_unixtime(UNIX_TIMESTAMP(DAY, 'yyyy-MM-dd'),'yyyy-MM-dd')as date)),
INPUT__FILE__NAME)Summery_log
group by 
DATE,
YEAR,
TYPE,
COUNTRY,
MONTH,
FILE_NAME;


DROP TABLE IF EXISTS cmrmsft.emt_display_old_log_count; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_old_log_count
(
   DATE			Date,
   YEAR			String,
   TYPE			String,
   COUNTRY		String,
   MONTH		String,
   RECORD_COUNT	Int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
);

INSERT OVERWRITE TABLE cmrmsft.emt_display_old_log_count
select DATE,YEAR,TYPE,COUNTRY,MONTH,RECORD_COUNT from
(select *,ROW_NUMBER() OVER(PARTITION BY YEAR,TYPE,COUNTRY,MONTH ORDER BY DATE DESC) as row
from cmrmsft.emt_display_summerylog) logtable where row=2;


DROP TABLE IF EXISTS cmrmsft.emt_display_new_log_count; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_new_log_count
(
   DATE			Date,
   YEAR			String,
   TYPE			String,
   COUNTRY		String,
   MONTH		String,
   RECORD_COUNT	Int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
);

INSERT OVERWRITE TABLE cmrmsft.emt_display_new_log_count
select DATE,YEAR,TYPE,COUNTRY,MONTH,RECORD_COUNT from
(select *,ROW_NUMBER() OVER(PARTITION BY YEAR,TYPE,COUNTRY,MONTH ORDER BY DATE DESC) as row
from cmrmsft.emt_display_summerylog) logtable where row=1;


DROP TABLE IF EXISTS cmrmsft.emt_display_total_log_count; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_total_log_count
(
   DATE			Date,
   YEAR			String,
   TYPE			String,
   COUNTRY		String,
   MONTH		String,
   RECORD_COUNT	Int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
) ;


INSERT OVERWRITE TABLE cmrmsft.emt_display_total_log_count
select
nl.DATE, 
nl.YEAR,
nl.TYPE,
nl.COUNTRY,
nl.MONTH,
Case when nl.RECORD_COUNT-COALESCE(ol.RECORD_COUNT, CAST(0 AS BIGINT))>0 
     then 0 
     else abs(nl.RECORD_COUNT-COALESCE(ol.RECORD_COUNT, CAST(0 AS BIGINT))) end as RECORD_COUNT
from cmrmsft.emt_display_new_log_count nl
left outer join cmrmsft.emt_display_old_log_count ol on nl.YEAR=ol.YEAR and nl.TYPE=ol.TYPE and nl.COUNTRY=ol.COUNTRY and nl.MONTH=ol.MONTH;



----------------------------------old script------------------------------------------

--create database if not exists cmrmsft;
--DROP TABLE IF EXISTS cmrmsft.emt_display_rawoldinput;
--CREATE EXTERNAL TABLE cmrmsft.emt_display_rawoldinput
--(
--  Country				String,
--  Day					String,
--  Media_Buy_Key			String,
--  Media_Buy_Name		String,
--  Media_Buy_Type		String,
--  Media_Units			String,
--  Product				String,
--  Creative_Key			String,
--  Creative_Name			String,
--  Creative_Click_URL	String,
--  Creative_Format		String,
--  Device_Category		String,
--  Campaign_Name			String,
--  Site_Group			String,
--  Media_Buy_Size		String,
--  Data_Source_Provider	String,
--  Creative_Concept		String,
--  Publisher				String,
--  Impressions			String,
--  Clicks				String,
--  Media_Costs			String,
--  Video_Fully_Played	String,
--  Rich_Impressions		String,
--  Rich_Clicks			String,
--  Total_Conversions		String,
--  CTR					String,
--  CPC					String,
--  Video_Views			String,
--  VTR					String,
--  CPCV					String,
--  IR					String,
--  Rich_CPI				String,
--  Rich_CTR				String
--)  
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE LOCATION 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt/input/processed/display/' TBLPROPERTIES("skip.header.line.count"="1");

--alter table cmrmsft.emt_display_rawoldinput set SERDEPROPERTIES ("serialization.null.format" = "");


--DROP TABLE IF EXISTS cmrmsft.emt_display_rawoldstg;
--CREATE EXTERNAL TABLE cmrmsft.emt_display_rawoldstg
--(
-- Country				String,
-- Day					Date,
-- Media_Buy_Key			String,
-- Media_Buy_Name			String,
-- Media_Buy_Type			String,
-- Media_Units			String,
-- Product				String,
-- Creative_Key			String,
-- Creative_Name			String,
-- Creative_Click_URL		String,
-- Creative_Format		String,
-- Device_Category		String,
-- Campaign_Name			String,
-- Site_Group				String,
-- Media_Buy_Size			String,
-- Data_Source_Provider	String,
-- Creative_Concept		String,
-- Publisher				String,
-- Impressions			String,
-- Clicks					String,
-- Media_Costs			String,
-- Video_Fully_Played		String,
-- Rich_Impressions		String,
-- Rich_Clicks			String,
-- Total_Conversions		String,
-- CTR					String,
-- CPC					String,
-- Video_Views			String,
-- VTR					String,
-- CPCV					String,
-- IR						String,
-- Rich_CPI				String,
-- Rich_CTR				String
--)  
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;

--INSERT OVERWRITE TABLE cmrmsft.emt_display_rawoldstg
--Select 	
--  Country,
--  cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy-MM-dd'),'yyyy-MM-dd')as date) Day,
--  Media_Buy_Key,
--  Media_Buy_Name,
--  Media_Buy_Type,
--  Media_Units,
--  Product,
--  Creative_Key,
--  Creative_Name,
--  Creative_Click_URL,
--  Creative_Format,
--  Device_Category,
--  Campaign_Name,
--  Site_Group,
--  Media_Buy_Size,
--  Data_Source_Provider,
--  Creative_Concept,
--  Publisher,
--  Impressions,
--  Clicks,
--  Media_Costs,
--  Video_Fully_Played,
--  Rich_Impressions,
--  Rich_Clicks,
--  Total_Conversions,
--  CTR,
--  CPC,
--  Video_Views,
--  VTR,
--  CPCV,
--  IR,
--  Rich_CPI,
--  Rich_CTR
--From cmrmsft.emt_display_rawoldinput
--where Day <> 'Empty';


--DROP TABLE IF EXISTS cmrmsft.emt_display_old_log_count; 
--CREATE EXTERNAL TABLE cmrmsft.emt_display_old_log_count
--(
--   Month			int,
--   Total_Count		int
--) 
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;

--INSERT OVERWRITE TABLE cmrmsft.emt_display_old_log_count
--select 
--Month(Day) Month,
--count(*) Total_Count
--from cmrmsft.emt_display_rawoldstg
--group by Month(Day);



--DROP TABLE IF EXISTS cmrmsft.emt_display_rawcurrentinput;
--CREATE EXTERNAL TABLE cmrmsft.emt_display_rawcurrentinput
--(
--  Country				String,
--  Day					String,
--  Media_Buy_Key			String,
--  Media_Buy_Name		String,
--  Media_Buy_Type		String,
--  Media_Units			String,
--  Product				String,
--  Creative_Key			String,
--  Creative_Name			String,
--  Creative_Click_URL	String,
--  Creative_Format		String,
--  Device_Category		String,
--  Campaign_Name			String,
--  Site_Group			String,
--  Media_Buy_Size		String,
--  Data_Source_Provider	String,
--  Creative_Concept		String,
--  Publisher				String,
--  Impressions			String,
--  Clicks				String,
--  Media_Costs			String,
--  Video_Fully_Played	String,
--  Rich_Impressions		String,
--  Rich_Clicks			String,
--  Total_Conversions		String,
--  CTR					String,
--  CPC					String,
--  Video_Views			String,
--  VTR					String,
--  CPCV					String,
--  IR					String,
--  Rich_CPI				String,
--  Rich_CTR				String
--) 
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE LOCATION 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt/input/current/display/' TBLPROPERTIES("skip.header.line.count"="1"); 

--alter table cmrmsft.emt_display_rawcurrentinput set SERDEPROPERTIES ("serialization.null.format" = "");


--DROP TABLE IF EXISTS cmrmsft.emt_display_rawcurrentstg;
--CREATE EXTERNAL TABLE cmrmsft.emt_display_rawcurrentstg
--(
--  Country				String,
--  Day					Date,
--  Media_Buy_Key			String,
--  Media_Buy_Name		String,
--  Media_Buy_Type		String,
--  Media_Units			String,
--  Product				String,
--  Creative_Key			String,
--  Creative_Name			String,
--  Creative_Click_URL	String,
--  Creative_Format		String,
--  Device_Category		String,
--  Campaign_Name			String,
--  Site_Group			String,
--  Media_Buy_Size		String,
--  Data_Source_Provider	String,
--  Creative_Concept		String,
--  Publisher				String,
--  Impressions			String,
--  Clicks				String,
--  Media_Costs			String,
--  Video_Fully_Played	String,
--  Rich_Impressions		String,
--  Rich_Clicks			String,
--  Total_Conversions		String,
--  CTR					String,
--  CPC					String,
--  Video_Views			String,
--  VTR					String,
--  CPCV					String,
--  IR					String,
--  Rich_CPI				String,
--  Rich_CTR				String,
--  Insert_Date			Date
--)  
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;


--INSERT OVERWRITE TABLE cmrmsft.emt_display_rawcurrentstg
--Select 	
--  Country,
--  cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy-MM-dd'),'yyyy-MM-dd')as date) Day,
--  Media_Buy_Key,
--  Media_Buy_Name,
--  Media_Buy_Type,
--  Media_Units,
--  Product,
--  Creative_Key,
--  Creative_Name,
--  Creative_Click_URL,
--  Creative_Format,
--  Device_Category,
--  Campaign_Name,
--  Site_Group,
--  Media_Buy_Size,
--  Data_Source_Provider,
--  Creative_Concept,
--  Publisher,
--  Impressions,
--  Clicks,
--  Media_Costs,
--  Video_Fully_Played,
--  Rich_Impressions,
--  Rich_Clicks,
--  Total_Conversions,
--  CTR,
--  CPC,
--  Video_Views,
--  VTR,
--  CPCV,
--  IR,
--  Rich_CPI,
--  Rich_CTR,
--  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) Insert_Date
--From cmrmsft.emt_display_rawcurrentinput
--where Country <> 'Empty' and Day <> 'Empty' and Media_Buy_Key<>'Empty' and Media_Buy_Type<>'Empty' and Media_Units<>'Empty' and Product<>'Empty' and 
--Creative_Key<>'Empty' and Device_Category<>'Empty' and Site_Group<>'Empty' and Media_Buy_Size<>'Empty' and Data_Source_Provider<>'Empty' and Creative_Concept<>'Empty'
--and Publisher<>'Empty' and Impressions<>'Empty' and Clicks<>'Empty' and Media_Costs<>'Empty' and Video_Fully_Played<>'Empty' and Rich_Impressions<>'Empty' and 
--Rich_Clicks<>'Empty' and Total_Conversions<>'Empty' and CTR<>'Empty' and CPC<>'Empty' and Video_Views<>'Empty' and IR<>'Empty' and Rich_CTR<>'Empty';


--DROP TABLE IF EXISTS cmrmsft.emt_display_rawcurrent;
--CREATE EXTERNAL TABLE cmrmsft.emt_display_rawcurrent
--(
--  Country				String,
--  Day					String,
--  Media_Buy_Key			String,
--  Media_Buy_Name		String,
--  Media_Buy_Type		String,
--  Media_Units			String,
--  Product				String,
--  Creative_Key			String,
--  Creative_Name			String,
--  Creative_Click_URL	String,
--  Creative_Format		String,
--  Device_Category		String,
--  Campaign_Name			String,
--  Site_Group			String,
--  Media_Buy_Size		String,
--  Data_Source_Provider	String,
--  Creative_Concept		String,
--  Publisher				String,
--  Impressions			String,
--  Clicks				String,
--  Media_Costs			String,
--  Video_Fully_Played	String,
--  Rich_Impressions		String,
--  Rich_Clicks			String,
--  Total_Conversions		String,
--  CTR					String,
--  CPC					String,
--  Video_Views			String,
--  VTR					String,
--  CPCV					String,
--  IR					String,
--  Rich_CPI				String,
--  Rich_CTR				String,
--  Insert_Date			Date
--)  
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;

--INSERT OVERWRITE TABLE cmrmsft.emt_display_rawcurrent
--Select 	
--  Country,
--  Day,
--  Media_Buy_Key,
--  Media_Buy_Name,
--  Media_Buy_Type,
--  Media_Units,
--  Product,
--  Creative_Key,
--  Creative_Name,
--  Creative_Click_URL,
--  Creative_Format,
--  Device_Category,
--  Campaign_Name,
--  Site_Group,
--  Media_Buy_Size,
--  Data_Source_Provider,
--  Creative_Concept,
--  Publisher,
--  Impressions,
--  Clicks,
--  Media_Costs,
--  Video_Fully_Played,
--  Rich_Impressions,
--  Rich_Clicks,
--  Total_Conversions,
--  CTR,
--  CPC,
--  Video_Views,
--  VTR,
--  CPCV,
--  IR,
--  Rich_CPI,
--  Rich_CTR,
--  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) Insert_Date
--From cmrmsft.emt_display_rawcurrentinput;


--DROP TABLE IF EXISTS cmrmsft.emt_display_new_log_count; 
--CREATE EXTERNAL TABLE cmrmsft.emt_display_new_log_count
--(
--   Month				int,
--   Total_Count			int,
--   INSERT_DATE			date
--) 
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;

--INSERT OVERWRITE TABLE cmrmsft.emt_display_new_log_count
--select 
--Month(Day) Month,
--count(*) Total_Count,
--TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) INSERT_DATE
--from cmrmsft.emt_display_rawcurrentstg
--group by Month(Day),TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP()));


--DROP TABLE IF EXISTS cmrmsft.emt_display_total_log_count; 
--CREATE EXTERNAL TABLE cmrmsft.emt_display_total_log_count
--(
--   Month				int,
--   Total_Count			decimal,
--   INSERT_DATE			date
--) 
--ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
--with serdeproperties (
--   "separatorChar" = ",",
--   "quoteChar"     = "\""   
--  ) 
--STORED AS TEXTFILE;


--INSERT OVERWRITE TABLE cmrmsft.emt_display_total_log_count
--select 
--nl.month,
--Case when nl.Total_Count-COALESCE(ol.Total_Count, CAST(0 AS BIGINT))>0 
--     then 0 
--     else abs(nl.Total_Count-COALESCE(ol.Total_Count, CAST(0 AS BIGINT))) end as Total_Count,
--     nl.INSERT_DATE
--from cmrmsft.emt_display_new_log_count nl
--left outer join cmrmsft.emt_display_old_log_count ol on nl.month=ol.mont

---------------------------------Cleansing part started----------------------------------------


DROP TABLE IF EXISTS cmrmsft.emt_display_empty; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_empty
(
  Country				String,
  Day					String,
  Media_Buy_Key			String,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			String,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			String,
  Clicks				String,
  Media_Costs			String,
  Video_Fully_Played	String,
  Rich_Impressions		String,
  Rich_Clicks			String,
  Total_Conversions		String,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_empty set SERDEPROPERTIES ("serialization.null.format" = "");


INSERT OVERWRITE TABLE cmrmsft.emt_display_empty
Select 	
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID from
(Select 	
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  NULL as VideoViews_25Q,
  NULL as VideoViews_50Q,
  NULL as VideoViews_75Q,
  NULL as CampaignID,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) CURRENT_DATE
FROM cmrmsft.emt_display_rawinput) RC
left outer join (select DATE,sum(RECORD_COUNT) RECORD_COUNT from cmrmsft.emt_display_total_log_count group by DATE) as TC  on RC.CURRENT_DATE = TC.DATE and TC.RECORD_COUNT<=5
where (Country is null OR Country = '' OR length(Country) = 0)  
or (Day = '' OR length(Day) = 0);
--or (Media_Buy_Key = '' OR length(Media_Buy_Key) = 0)
--or (Media_Buy_Type = '' OR length(Media_Buy_Type) = 0)
--or (Media_Units = '' OR length(Media_Units) = 0)
--or (Product = '' OR length(Product) = 0);
--or (Creative_Key = '' OR length(Creative_Key) = 0)
--or (Device_Category = '' OR length(Device_Category) = 0)
--or (Site_Group = '' OR length(Site_Group) = 0)
--or (Media_Buy_Size = '' OR length(Media_Buy_Size) = 0  )
--or (Data_Source_Provider = '' OR length(Data_Source_Provider) = 0)
--or (Creative_Concept = '' OR length(Creative_Concept) = 0)
--or (Publisher = '' OR length(Publisher) = 0)
--or (Impressions = '' OR length(Impressions) = 0 )
--or (Clicks = '' OR length(Clicks) = 0)
--or (Media_Costs = '' OR length(Media_Costs) = 0)
--or (Video_Fully_Played = '' OR length(Video_Fully_Played) = 0)
--or (Rich_Impressions = '' OR length(Rich_Impressions) = 0)
--or (Rich_Clicks = '' OR length(Rich_Clicks) = 0)
--or (Total_Conversions = '' OR length(Total_Conversions) = 0)
--or (CTR = '' OR length(CTR) = 0);
--or (CPC = '' OR length(CPC) = 0)
--or (Video_Views = '' OR length(Video_Views) = 0)
--or (IR = '' OR length(IR) = 0)
--or (Rich_CTR = '' OR length(Rich_CTR) = 0);



--where Country = 'Empty' or Day = 'Empty' or Media_Buy_Key='Empty' or Media_Buy_Type='Empty' or Media_Units='Empty' or Product='Empty' or 
--Creative_Key='Empty' or Device_Category='Empty' or Site_Group='Empty' or Media_Buy_Size='Empty' or Data_Source_Provider='Empty' or Creative_Concept='Empty'
--or Publisher='Empty' or Impressions='Empty' or Clicks='Empty' or Media_Costs='Empty' or Video_Fully_Played='Empty' or Rich_Impressions='Empty' or 
--Rich_Clicks='Empty' or Total_Conversions='Empty' or CTR='Empty' or CPC='Empty' or Video_Views='Empty' or IR='Empty' or Rich_CTR='Empty';

DROP TABLE IF EXISTS cmrmsft.emt_display_nonempty; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_nonempty
(
  Country				String,
  Day					Date,
  Media_Buy_Key			Bigint,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			Bigint,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			int,
  Clicks				int,
  Media_Costs			int,
  Video_Fully_Played	int,
  Rich_Impressions		int,
  Rich_Clicks			int,
  Total_Conversions		int,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_nonempty set SERDEPROPERTIES ("serialization.null.format" = "");


INSERT OVERWRITE TABLE cmrmsft.emt_display_nonempty
Select 	
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID from
(Select 	
  Country,
  case when length(regexp_extract(Day,'[0-9][0-9][.][0-1][0-9][.][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'dd.MM.yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][.][0-1][0-9][.][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'd.MM.yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][.][0-9][.][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'dd.M.yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][.][0-9][.][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'd.M.yyyy'),'yyyy-MM-dd')as date)
		
	when length(regexp_extract(Day,'[0-9][0-9][/][0-9][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM/dd/yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][/][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM/d/yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-9][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M/dd/yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M/d/yyyy'),'yyyy-MM-dd')as date)
		
	when length(regexp_extract(Day,'[0-1][0-9][-][0-9][0-9][-][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM-dd-yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-1][0-9][-][0-9][-][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM-d-yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][-][0-9][0-9][-][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M-dd-yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][-][0-9][-][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M-d-yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9]-[a-zA-Z]{3}-[0-9][0-9]',0))>0
		then cast(from_unixtime(UNIX_TIMESTAMP(Day,'dd-MMM-yy'),'yyyy-MM-dd') as date)
	when length(regexp_extract(Day,'[0-9]-[a-zA-Z]{3}-[0-9][0-9]',0))>0
		then cast(from_unixtime(UNIX_TIMESTAMP(Day,'d-MMM-yy'),'yyyy-MM-dd') as date)
	when length(regexp_extract(Day,'[0-9][0-9]-[a-zA-Z]{3}-[0-9][0-9][0-9][0-9]',0))>0
		then cast(from_unixtime(UNIX_TIMESTAMP(Day,'dd-MMM-yyyy'),'yyyy-MM-dd') as date)
	when length(regexp_extract(Day,'[0-9]-[a-zA-Z]{3}-[0-9][0-9][0-9][0-9]',0))>0
		then cast(from_unixtime(UNIX_TIMESTAMP(Day,'d-MMM-yyyy'),'yyyy-MM-dd') as date)
		
	when length(regexp_extract(Day,'[0-1][0-9][/][0-9][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM/dd/yy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-1][0-9][/][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'MM/d/yy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-9][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M/dd/yy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'M/d/yy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][/][0-1][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'dd/MM/yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-1][0-9][/][0-9][0-9][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'd/MM/yyyy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][/][0-1][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'd/MM/yy'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][0-9][0-9][/][0-1][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy/MM/dd'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][0-9][0-9][/][0-9][/][0-9][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy/M/dd'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][0-9][0-9][/][0-1][0-9][/][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy/MM/d'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[0-9][0-9][/][0-9][0-9][/][0-9][/][0-9]',0))>0 
		then cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy/M/d'),'yyyy-MM-dd')as date)
	when length(regexp_extract(Day,'[a-zA-Z]{3}[0-9][0-9]/[0-9][0-9]',0))>0
        then cast(from_unixtime(UNIX_TIMESTAMP(Day,'MMMdd/yy'),'yyyy-MM-dd') as date)
	when length(regexp_extract(Day,'[0-9][0-9][a-zA-Z]{3}[0-9][0-9]',0))>0
        then cast(from_unixtime(UNIX_TIMESTAMP(Day,'ddMMMyy'),'yyyy-MM-dd') as date)
	when length(regexp_extract(Day,'[0-9][0-9][a-zA-Z]{3}[0-9][0-9]-[0-9][0-9][a-zA-Z]{3}[0-9][0-9]',0))>0
		then cast(from_unixtime(UNIX_TIMESTAMP((SUBSTR(Day,9,15)),'ddMMMyy'),'yyyy-MM-dd') as date)
else cast(from_unixtime(UNIX_TIMESTAMP(Day, 'yyyy-MM-dd'),'yyyy-MM-dd')as date) end Day,
  cast(Media_Buy_Key as Bigint) Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  cast(Creative_Key as Bigint) Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  cast(Impressions as int) Impressions,
  cast(Clicks as int) Clicks,
  cast(Media_Costs as int) Media_Costs,
  cast(Video_Fully_Played as int) Video_Fully_Played,
  cast(Rich_Impressions as int) Rich_Impressions,
  cast(Rich_Clicks as int) Rich_Clicks,
  cast(Total_Conversions as int) Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  NULL as VideoViews_25Q,
  NULL as VideoViews_50Q,
  NULL as VideoViews_75Q,
  NULL as CampaignID,
  TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP())) CURRENT_DATE
FROM cmrmsft.emt_display_rawinput) RC
left outer join (select DATE,sum(RECORD_COUNT) RECORD_COUNT from cmrmsft.emt_display_total_log_count group by DATE) as TC  on RC.CURRENT_DATE = TC.DATE and TC.RECORD_COUNT<=5
where (Country <> '' OR length(Country) <> 0) 
and (Day <> '' OR length(Day) <> 0);
--and (Media_Buy_Key <> '' OR length(Media_Buy_Key) <> 0)
--and (Media_Buy_Type <> '' OR length(Media_Buy_Type) <> 0)
--and (Media_Units <> '' OR length(Media_Units) <> 0)
--and (Product <> '' OR length(Product) <> 0);
--and (Creative_Key <> '' OR length(Creative_Key) <> 0)
--and (Device_Category <> '' OR length(Device_Category) <> 0)
--and (Site_Group <> '' OR length(Site_Group) <> 0)
--and (Media_Buy_Size <> '' OR length(Media_Buy_Size) <> 0)
--and (Data_Source_Provider <> '' OR length(Data_Source_Provider) <> 0)
--and (Creative_Concept <> '' OR length(Creative_Concept) <> 0)
--and (Publisher <> '' OR length(Publisher) <> 0)
--and (Impressions <> '' OR length(Impressions) <> 0)
--and (Clicks <> '' OR length(Clicks) <> 0)
--and (Media_Costs <> '' OR length(Media_Costs) <> 0)
--and (Video_Fully_Played <> '' OR length(Video_Fully_Played) <> 0)
--and (Rich_Impressions <> '' OR length(Rich_Impressions) <> 0)
--and (Rich_Clicks <> '' OR length(Rich_Clicks) <> 0)
--and (Total_Conversions <> '' OR length(Total_Conversions) <> 0)
--and (CTR <> '' OR length(CTR) <> 0);
--and (CPC <> '' OR length(CPC) <> 0);
--and (Video_Views <> '' OR length(Video_Views) <> 0)
--and (IR <> '' OR length(IR) <> 0)
--and (Rich_CTR <> '' OR length(Rich_CTR) <> 0);



--where Country <> 'Empty' and Day <> 'Empty' and Media_Buy_Key<>'Empty' and Media_Buy_Type<>'Empty' and Media_Units<>'Empty' and Product<>'Empty' and 
--Creative_Key<>'Empty' and Device_Category<>'Empty' and Site_Group<>'Empty' and Media_Buy_Size<>'Empty' and Data_Source_Provider<>'Empty' and Creative_Concept<>'Empty'
--and Publisher<>'Empty' and Impressions<>'Empty' and Clicks<>'Empty' and Media_Costs<>'Empty' and Video_Fully_Played<>'Empty' and Rich_Impressions<>'Empty' and 
--Rich_Clicks<>'Empty' and Total_Conversions<>'Empty' and CTR<>'Empty' and CPC<>'Empty' and Video_Views<>'Empty' and IR<>'Empty' and Rich_CTR<>'Empty';


DROP TABLE IF EXISTS cmrmsft.emt_display_stg; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_stg
(
  Country				String,
  Day					Date,
  Media_Buy_Key			Bigint,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			Bigint,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			int,
  Clicks				int,
  Media_Costs			int,
  Video_Fully_Played	int,
  Rich_Impressions		int,
  Rich_Clicks			int,
  Total_Conversions		int,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_stg set SERDEPROPERTIES ("serialization.null.format" = "");


INSERT OVERWRITE TABLE cmrmsft.emt_display_stg
Select 	
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID
FROM cmrmsft.emt_display_nonempty;


DROP TABLE IF EXISTS cmrmsft.emt_display_stg_key; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_stg_key
(
  Country				String,
  Day					Date,
  Media_Buy_Key			Bigint,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			Bigint,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			int,
  Clicks				int,
  Media_Costs			int,
  Video_Fully_Played	int,
  Rich_Impressions		int,
  Rich_Clicks			int,
  Total_Conversions		int,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String,
  Key					String
 
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_stg_key set SERDEPROPERTIES ("serialization.null.format" = "");


INSERT OVERWRITE TABLE cmrmsft.emt_display_stg_key
Select 	
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID,
  concat(Country,'-',Day,'-',Media_Buy_Key,'-',Media_Buy_Name,'-',Media_Buy_Type,'-',Media_Units,'-',Product,'-',Creative_Key,'-',Creative_Name,'-',Creative_Click_URL,'-',Creative_Format,'-',Device_Category,
  '-',Campaign_Name,'-',Site_Group,'-',Media_Buy_Size,'-',Data_Source_Provider,'-',Creative_Concept,'-',Publisher,'-',Impressions,'-',Clicks,'-',Media_Costs,'-',
  Video_Fully_Played,'-',Rich_Impressions,'-',Rich_Clicks,'-',Total_Conversions,'-',CTR,'-',CPC,'-',Video_Views,'-',VTR,'-',CPCV,'-',IR,'-',Rich_CPI,'-',Rich_CTR) KEY
FROM cmrmsft.emt_display_stg;

DROP TABLE IF EXISTS cmrmsft.emt_display_clean; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_clean
(
  Country				String,
  Day					Date,
  Media_Buy_Key			Bigint,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			Bigint,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			int,
  Clicks				int,
  Media_Costs			int,
  Video_Fully_Played	int,
  Rich_Impressions		int,
  Rich_Clicks			int,
  Total_Conversions		int,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_clean set SERDEPROPERTIES ("serialization.null.format" = "");

INSERT OVERWRITE TABLE cmrmsft.emt_display_clean
select 
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID
 from (
select *,row_number() over(partition by KEY order by Day) as row
from cmrmsft.emt_display_stg_key) table where row>=1;

DROP TABLE IF EXISTS cmrmsft.emt_display_dup; 
CREATE EXTERNAL TABLE cmrmsft.emt_display_dup
(
  Country				String,
  Day					Date,
  Media_Buy_Key			Bigint,
  Media_Buy_Name		String,
  Media_Buy_Type		String,
  Media_Units			String,
  Product				String,
  Creative_Key			Bigint,
  Creative_Name			String,
  Creative_Click_URL	String,
  Creative_Format		String,
  Device_Category		String,
  Campaign_Name			String,
  Site_Group			String,
  Media_Buy_Size		String,
  Data_Source_Provider	String,
  Creative_Concept		String,
  Publisher				String,
  Impressions			int,
  Clicks				int,
  Media_Costs			int,
  Video_Fully_Played	int,
  Rich_Impressions		int,
  Rich_Clicks			int,
  Total_Conversions		int,
  CTR					String,
  CPC					String,
  Video_Views			String,
  VTR					String,
  CPCV					String,
  IR					String,
  Rich_CPI				String,
  Rich_CTR				String,
  VideoViews_25Q		String,
  VideoViews_50Q		String,
  VideoViews_75Q		String,
  CampaignID			String
)  
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
   "separatorChar" = ",",
   "quoteChar"     = "\""   
  ) 
STORED AS TEXTFILE;

alter table cmrmsft.emt_display_dup set SERDEPROPERTIES ("serialization.null.format" = "");


INSERT OVERWRITE TABLE cmrmsft.emt_display_dup
select 
  Country,
  Day,
  Media_Buy_Key,
  Media_Buy_Name,
  Media_Buy_Type,
  Media_Units,
  Product,
  Creative_Key,
  Creative_Name,
  Creative_Click_URL,
  Creative_Format,
  Device_Category,
  Campaign_Name,
  Site_Group,
  Media_Buy_Size,
  Data_Source_Provider,
  Creative_Concept,
  Publisher,
  Impressions,
  Clicks,
  Media_Costs,
  Video_Fully_Played,
  Rich_Impressions,
  Rich_Clicks,
  Total_Conversions,
  CTR,
  CPC,
  Video_Views,
  VTR,
  CPCV,
  IR,
  Rich_CPI,
  Rich_CTR,
  VideoViews_25Q,
  VideoViews_50Q,
  VideoViews_75Q,
  CampaignID
 from (
select *,row_number() over(partition by KEY order by Day) as row
from cmrmsft.emt_display_stg_key) table where row=0;


INSERT OVERWRITE DIRECTORY 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/summery_log/FY2015' SELECT * FROM cmrmsft.emt_display_summerylog where Year is not null;

INSERT OVERWRITE DIRECTORY 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/log_count/FY2015' SELECT * FROM cmrmsft.emt_display_total_log_count;

INSERT OVERWRITE DIRECTORY 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/clean/FY2015' SELECT * FROM cmrmsft.emt_display_clean;

INSERT OVERWRITE DIRECTORY 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/duplicate/FY2015' SELECT * FROM cmrmsft.emt_display_dup;

INSERT OVERWRITE DIRECTORY 'wasb://cmrmsftblob@cmrbohadoop.blob.core.windows.net/emt_new/output/display/empty/FY2015' SELECT * FROM cmrmsft.emt_display_empty;

