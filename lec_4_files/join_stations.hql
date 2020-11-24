create table delays (
  year smallint, month tinyint, day tinyint, carrier string, flight string,
  origin_name string, origin_city string, origin_code string, dep_delay string,
  dest_name string, dest_city string, dest_code string, arr_delay string)
  stored as orc;

insert overwrite table delays
  select t.year as year, t.month as month, t.dayofmonth as day,
  t.carrier as carrier, t.flightnum as flight,
  t.origin as origin_name, t.origincityname as origin_city, so.code as origin_code, t.depdelay as dep_delay,
  t.dest as dest_name, t.destcityname as dest_city, sd.code as dest_code, t.arrdelay as arr_delay
  from ontime t join stations so join stations sd
    on t.origin = so.name and t.dest = sd.name;
