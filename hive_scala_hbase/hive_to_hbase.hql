create table reid7_transport_weather_daily_hbase (
  date_key string,
  year string,
  month string,
  day string,
  avg_precipitation float,
  precip_cat string,
  avg_snow float,
  snow_cat string,
  avg_temp smallint,
  trip_duration bigint,
  trip_duration_avg_mo bigint,
  total_trips bigint,
  total_trips_avg_mo bigint,
  subscibers bigint,
  non_subscribers bigint,
  average_age bigint,
  total_bus_trips bigint,
  total_bus_avg_mo bigint,
  total_rail_trips bigint,
  total_rail_avg_mo bigint,
  total_rides bigint,
  total_rides_avg_mo bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:day,stat:avg_precipitation#b,stat:precip_cat,stat:avg_snow#b,stat:snow_cat,stat:avg_temp#b,stat:trip_duration#b,stats:trip_duration_avg_mo#b,stat:total_trips#b,stat:total_trips_avg_mo#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,stat:total_bus_trips#b,stat:total_bus_avg_mo#b,stat:total_rail_trips#b,stat:total_rail_avg_mo#b,stat:total_rides#b,stat:total_rides_avg_mo#b')
TBLPROPERTIES ('hbase.table.name' = 'reid7_transport_weather_daily');


insert overwrite table reid7_transport_weather_daily_hbase
select * from reid7_transport_weather_daily;


create table reid7_transport_weather_monthly_hbase (
  date_key string,
  month string,
  year string,
  avg_precipitation float,
  avg_snow float,
  avg_temp smallint,
  trip_duration bigint,
  total_trips bigint,
  subscibers bigint,
  non_subscribers bigint,
  average_age bigint,
  total_bus_trips bigint,
  total_rail_trips bigint,
  total_rides bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:avg_precipitation#b,stat:avg_snow#b,stat:avg_temp#b,stat:trip_duration#b,stat:total_trips#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,stat:total_bus_trips#b,stat:total_rail_trips#b,stat:total_rides#b,')
TBLPROPERTIES ('hbase.table.name' = 'reid7_transport_weather_monthly');

insert overwrite table reid7_transport_weather_monthly_hbase
select * from reid7_transport_weather_monthly;
