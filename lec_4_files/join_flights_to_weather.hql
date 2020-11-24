create table flights_and_weather (
  year smallint, month tinyint, day tinyint, carrier string, flight string,
  origin_name string, origin_city string, origin_code string, dep_delay double,
  dest_name string, dest_city string, dest_code string, arr_delay double,
  mean_temperature double, mean_visibility double, mean_windspeed double,
  fog boolean, rain boolean, snow boolean, hail boolean, thunder boolean, tornado boolean,
  fog_delay double, rain_delay double, snow_delay double, hail_delay double,
  thunder_delay double, tornado_delay double, clear_delay double) stored as orc;

insert overwrite table flights_and_weather
  select d.year as year, d.month as month, d.day as day, d.carrier as carrier, d.flight as flight,
  d.origin_name as origin_name, d.origin_city as origin_city, d.origin_code as origin_code, d.dep_delay as dep_delay,
  d.dest_name as dest_name, d.dest_city as dest_city, d.dest_code as dest_code, d.arr_delay as arr_delay,
  w.meantemperature as mean_temperature, w.meanvisibility as mean_visibility, w.meanwindspeed as mean_windspeed,
  w.fog as fog, w.rain as rain, w.snow as snow, w.hail as hail, w.thunder as thunder, w.tornado as tornado,
  if(w.fog,d.dep_delay,0) as fog_delay, if(w.rain,d.dep_delay,0) as rain_delay, if(w.snow, d.dep_delay, 0) as snow_delay,
  if(w.hail, d.dep_delay, 0) as hail_delay, if(w.thunder, d.dep_delay, 0) as thunder_delay,
  if(w.tornado,  d.dep_delay, 0) as tornado_delay,
  if(w.fog or w.rain or w.snow or w.hail or w.thunder or w.tornado, 0, d.dep_delay) as clear_delay
  from delays d join weathersummary w
  on d.year = w.year and d.month = w.month and d.day = w.day and d.origin_code = w.station;

