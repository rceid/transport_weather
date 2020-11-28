beeline -u jdbc:hive2://localhost:10000/default -n hadoop -d org.apache.hive.jdbc.HiveDriver
-- weather csv in HDFS to Hive:

create external table reid7_weather_csv(
  Station string,
  name string,
  weather_date string,
  days_precip smallint,
  multidayprecip smallint,
  precipitation float,
  snow_amount float,
  snowdepth float,
  avg_temp smallint,
  max_temp smallint,
  min_temp smallint,
  tmp_at_obs smallint,
  fog boolean,
  heavy_fog boolean,
  thunder boolean,
  ice boolean,
  hail boolean,
  glaze boolean,
  haze boolean,
  snow boolean,
  high_winds boolean,
  mist boolean,
  drizzle boolean,
  rain boolean,
  freezing_rain boolean,
  snow_crystals boolean,
  unknown_precip boolean,
  ice_fog boolean)
  row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/tmp/reid7/weather';




---TEST Query
select * from reid7_divvy_csv limit 10;


-- Create an ORC table for weather data (Note "stored as ORC" at the end)
create external table reid7_weather(
  Station string,
  name string,
  weather_date string,
  days_precip smallint,
  multidayprecip smallint,
  precipitation float,
  snow_amount float,
  snowdepth float,
  avg_temp smallint,
  max_temp smallint,
  min_temp smallint,
  tmp_at_obs smallint,
  fog boolean,
  heavy_fog boolean,
  thunder boolean,
  ice boolean,
  hail boolean,
  glaze boolean,
  haze boolean,
  snow boolean,
  high_winds boolean,
  mist boolean,
  drizzle boolean,
  rain boolean,
  freezing_rain boolean,
  snow_crystals boolean,
  unknown_precip boolean,
  ice_fog boolean)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_weather select * from reid7_weather_csv;