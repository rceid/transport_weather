-- CTA csv in HDFS to Hive:
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

create external table reid7_CTA_daily_csv(
  service_date string,
  day_type string,
  total_bus int,
  total_rail int,
  total_rides int)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/reid7/CTA';


---TEST Query
select * from reid7_CTA_daily_csv limit 10;


-- Create an ORC table for CTA data (Note "stored as ORC" at the end)
create table reid7_CTA_daily(
  service_date string,
  day_type string,
  total_bus int,
  total_rail int,
  total_rides int)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_CTA_daily select * from reid7_CTA_daily_csv;
