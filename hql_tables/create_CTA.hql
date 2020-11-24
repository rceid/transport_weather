-- divvy csv in HDFS to Hive:
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

create external table reid7_CTA_csv(
  route string,
  routename string,
  mm_dd_yy string,
  avg_weekday float,
  avg_sat float,
  avg_sun_hday float,
  month_total int)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/reid7/CTA';


---TEST Query
select * from reid7_CTA_csv limit 10;


-- Create an ORC table for divvy data (Note "stored as ORC" at the end)
create table reid7_CTA(
  route string,
  routename string,
  mm_dd_yy string,
  avg_weekday float,
  avg_sat float,
  avg_sun_hday float,
  month_total int)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_CTA select * from reid7_CTA_csv;
