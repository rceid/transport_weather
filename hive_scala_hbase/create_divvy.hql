-- divvy csv in HDFS to Hive:
beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver

create external table reid7_divvy_csv(
  TripId string,
  StartTime string,
  StopTime string,
  BikeId string,
  TripDuration smallint,
  fromStationId string,
  fromStationName string,
  toStationId string,
  UserType string,
  Gender string,
  BirthYear smallint,
  fromLatitude float,
  fromLongitude float,
  fromLocation string,
  toLatitude float,
  toLongitude float,
  toLocation string)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/reid7/divvy';


---TEST Query
select * from reid7_divvy_csv limit 10;


-- Create an ORC table for divvy data (Note "stored as ORC" at the end)
create table reid7_divvy(
  TripId string,
  StartTime string,
  StopTime string,
  BikeId string,
  TripDuration smallint,
  fromStationId string,
  fromStationName string,
  toStationId string,
  toStationName,
  UserType string,
  Gender string,
  BirthYear smallint,
  fromLatitude float,
  fromLongitude float,
  fromLocation string,
  toLatitude float,
  toLongitude float,
  toLocation string)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_divvy select * from reid7_divvy_csv;