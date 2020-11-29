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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:day,stat:avg_precipitation,
stat:precip_cat,stat:avg_snow,stat:snow_cat,stat:avg_temp,stat:trip_duration#b,stat:trip_duration_avg_mo#b,stat:total_trips#b,stat:total_trips_avg_mo#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,stat:total_bus_trips#b,stat:total_bus_avg_mo#b,stat:total_rail_trips#b,stat:total_rail_avg_mo#b,stat:total_rides#b,stat:total_rides_avg_mo#b')
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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:avg_precipitation,stat:avg_snow,stat:avg_temp,stat:trip_duration#b,
stat:total_trips#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,stat:total_bus_trips#b,stat:total_rail_trips#b,stat:total_rides#b,')
TBLPROPERTIES ('hbase.table.name' = 'reid7_transport_weather_monthly');

insert overwrite table reid7_transport_weather_monthly_hbase
select * from reid7_transport_weather_monthly;


create table reid7_daily_snow_hbase (
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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:day,stat:avg_precipitation,
stat:precip_cat,stat:avg_snow,stat:snow_cat,stat:avg_temp,stat:trip_duration#b,stat:trip_duration_avg_mo#b,
stat:total_trips#b,stat:total_trips_avg_mo#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,
stat:total_bus_trips#b,stat:total_bus_avg_mo#b,stat:total_rail_trips#b,stat:total_rail_avg_mo#b,stat:total_rides#b,
stat:total_rides_avg_mo#b')
TBLPROPERTIES ('hbase.table.name' = 'reid7_daily_snow');

insert overwrite table reid7_daily_snow_hbase
select snow_cat || "-" || date_key, year, month, day, avg_precipitation, precip_cat, avg_snow, snow_cat, avg_temp, trip_duration, avg_mo_duration,
 total_trips,avg_mo_trips,subscribers,non_subscribers, average_age,total_bus,avg_mo_bus,total_rail,avg_mo_train,total_rides,
 avg_mo_rides from reid7_transport_weather_daily order by year, month, day;


create table reid7_daily_precip_hbase (
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
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year,stat:month,stat:day,stat:avg_precipitation,
stat:precip_cat,stat:avg_snow,stat:snow_cat,stat:avg_temp,stat:trip_duration#b,stat:trip_duration_avg_mo#b,
stat:total_trips#b,stat:total_trips_avg_mo#b,stat:subscibers#b,stat:non_subscribers#b,stat:average_age#b,
stat:total_bus_trips#b,stat:total_bus_avg_mo#b,stat:total_rail_trips#b,stat:total_rail_avg_mo#b,stat:total_rides#b,
stat:total_rides_avg_mo#b')
TBLPROPERTIES ('hbase.table.name' = 'reid7_daily_precip');

insert overwrite table reid7_daily_precip_hbase
select precip_cat || "-" || date_key, year, month, day, avg_precipitation, precip_cat, avg_snow, snow_cat, avg_temp, trip_duration, avg_mo_duration,
 total_trips,avg_mo_trips,subscribers,non_subscribers, average_age,total_bus,avg_mo_bus,total_rail,avg_mo_train,total_rides,
 avg_mo_rides from reid7_transport_weather_daily order by year, month, day;


create table reid7_yrs_hbase(
  year_key string,
  year string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:year')
TBLPROPERTIES ('hbase.table.name' = 'reid7_yrs');

insert overwrite table reid7_yrs_hbase
select distinct year, year from reid7_transport_weather_monthly;



create table reid7_snow_hbase(
  snow_key string,
  snow_cat string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:snow_cat')
TBLPROPERTIES ('hbase.table.name' = 'reid7_snow_categories');

insert overwrite table reid7_snow_hbase
select distinct snow_cat, snow_cat from reid7_transport_weather_daily;


create table reid7_precip_hbase(
  precip_key string,
  precip_cat string)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,stat:precip_cat')
TBLPROPERTIES ('hbase.table.name' = 'reid7_precip_categories');

insert overwrite table reid7_precip_hbase
select distinct precip_cat, precip_cat from reid7_transport_weather_daily;
