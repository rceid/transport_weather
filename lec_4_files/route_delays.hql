create table route_delays (
  origin_name string, dest_name string, flights int,
  fog_flights int, fog_delays bigint,
  rain_flights int, rain_delays bigint,
  snow_flights int, snow_delays bigint,
  hail_flights int, hail_delays bigint,
  thunder_flights int, thunder_delays bigint,
  tornado_flights int, tornado_delays bigint,
  clear_flights int, clear_delays bigint);

insert overwrite table route_delays
  select origin_name, dest_name, count(1),
  count(if(fog, 1, null)), sum(fog_delay),
  count(if(rain, 1, null)), sum(rain_delay),
  count(if(snow, 1, null)), sum(snow_delay),
  count(if(hail, 1, null)), sum(hail_delay),
  count(if(thunder, 1, null)), sum(thunder_delay),
  count(if(tornado, 1, null)), sum(tornado_delay),
  count(if(!fog and !rain and !snow and !hail and !thunder and !tornado, 1, null)), sum(clear_delay)
  from flights_and_weather
  group by origin_name, dest_name;