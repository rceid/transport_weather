create table reid7_CTA_clean(
  route string,
  routename string,
  month string,
  year string,
  avg_weekday float,
  avg_sat float,
  avg_sun_hday float,
  month_total int)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_CTA_clean
SELECT route, routename, SUBSTR(mm_dd_yy, 1, 2), SUBSTR(mm_dd_yy, 7, 4), avg_weekday, avg_sat, avg_sun_hday, month_total from reid7_CTA;

create table reid7_divvy_clean(
  TripId string,
  month string,
  day string,
  year string,
  TripDuration smallint,
  fromStationName string,
  toStationName string,
  UserType string,
  Gender string,
  BirthYear smallint)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_divvy_clean 
select tripid, SUBSTR(starttime, 1, 2), SUBSTR(starttime, 4, 2), SUBSTR(starttime, 7, 4), tripduration, fromstationname, tostationname, usertype, gender, birthyear from reid7_divvy;


create external table reid7_weather_clean(
  Station string,
  name string,
  year string,
  month string,
  day string,
  precipitation float,
  snow_amount float,
  avg_temp smallint,
  max_temp smallint,
  min_temp smallint,
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

insert overwrite table reid7_weather_clean
select station, name, substr(weather_date, 1, 4),substr(weather_date, 6, 2), substring(weather_date, 9,2), precipitation, snow_amount, 
avg_temp, max_temp, min_temp, fog, heavy_fog, thunder, ice, hail, glaze, haze, snow, high_winds, mist, drizzle, rain, freezing_rain, snow_crystals,unknown_precip, ice_fog from reid7_weather;
