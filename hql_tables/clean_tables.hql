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

create table reid7_divvy(
  TripId string,
  month string,
  day string,
  year string,
  StartTime string,
  StopTime string,
  TripDuration smallint,
  fromStationName string,
  UserType string,
  Gender string,
  BirthYear smallint,
  toLocation string)
  stored as orc;


-- Copy the CSV table to the ORC table
insert overwrite table reid7_divvy select * from reid7_divvy_csv;
select tripid, SUBSTR(starttime, 1, 2) as month, SUBSTR(starttime, 4, 2) as day, SUBSTR(starttime, 7, 4) as year, tripduration, fromstationname, tostationname, usertype, gender, birthyear from reid7_divvy;