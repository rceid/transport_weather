import org.apache.spark.sql.SaveMode

//loading divvy and cta data, parsing dates
val divvy = spark.sql(
  """select tripid, SUBSTR(starttime, 1, 2) as month, SUBSTR(starttime, 4, 2) as day,
    SUBSTR(starttime, 7, 4) as year, tripduration, fromstationname, tostationname, usertype, gender,
    birthyear from reid7_divvy;""")
val cta = spark.sql(
  """SELECT SUBSTR(service_date, 1, 2) as month, SUBSTR(service_date, 4, 2) as day,
    SUBSTR(service_date, 7, 4) as year, total_bus, total_rail, total_rides from reid7_CTA_daily;""")

divvy.createOrReplaceTempView("divvy")
cta.createOrReplaceTempView("cta")

//create daily views for divvy, cta is already daily
val divvy_daily = spark.sql(
  """select year, month, day, sum(tripduration) as trip_duration, count(1) as total_trips,
    count(if(usertype == "Subscriber", 1, null)) as subscribers, count(if(usertype == "Customer", 1, null)) as non_subscribers,
    cast(2020 - avg(birthyear) as int) as average_age from divvy group by day, month, year;""")
divvy_daily.createOrReplaceTempView("divvy_daily")

//creating monthly views for cta and divvy
val cta_mo = spark.sql(
  """select month, year, sum(total_bus) as total_bus, sum(total_rail) as total_rail,
    sum(total_rides) as total_rides from cta group by month, year;""")
val divvy_mo = spark.sql(
  """select month, year, sum(tripduration) as trip_duration, count(1) as total_trips,
    count(if(usertype == "Subscriber", 1, null)) as subscribers,
    count(if(usertype == "Customer", 1, null)) as non_subscribers, cast(2020 - avg(birthyear) as int) as average_age from
     divvy group by month, year;""")

cta_mo.createOrReplaceTempView("cta_mo")
divvy_mo.createOrReplaceTempView("divvy_mo")

//loading, cleaning weather
val weather = spark.sql(
  """select SUBSTR(weather_date, 1, 4) as year, SUBSTR(weather_date, 6, 2) as month,
    SUBSTR(weather_date, 9, 2) as day, precipitation, snow_amount,  avg_temp, if(fog==true,1, null) as fog,
    if(mist==true, 1, null) as mist, if(thunder==true, 1, null) as thunder, if(ice==true, 1, null) as ice,
    if(hail==true, 1, null) as hail, if(snow==true, 1, null) as snow, if(high_winds==true, 1, null) as high_winds,
    if(rain==true, 1, null) as rain, if(freezing_rain==true, 1, null) as freezing_rain from reid7_weather;""")
weather.createOrReplaceTempView("weather")

//monthly and daily views for weather
val weather_daily = spark.sql(
  """select year, month, day, round(avg(precipitation), 2) as avg_precipitation,
    round(avg(snow_amount), 2) as avg_snow, cast(avg(avg_temp) as int) as avg_temp from weather group by
    day, month, year;""")
weather_daily.createOrReplaceTempView("weather_daily")

val weather_monthly = spark.sql(
  """select year, month, round(avg(avg_precipitation), 2) as avg_precipitation,
    round(avg(avg_snow), 2) as avg_snow, cast(avg(avg_temp) as int) as avg_temp from weather_daily group by
    month, year;""")
weather_monthly.createOrReplaceTempView("weather_monthly")


//bring everything together on a monthly level
val monthly_totals = spark.sql(
  """select w.year || "-" || w.month as date_key, w.month, w.year, avg_precipitation, avg_snow, avg_temp,
    trip_duration, total_trips, subscribers, non_subscribers, average_age, total_bus, total_rail,
    total_rides from weather_monthly w join divvy_mo d join cta_mo c on w.month = c.month and
    w.year = c.year and w.month = d.month and w.year = d.year order by year, month""")
monthly_totals.createOrReplaceTempView("monthly_totals")

monthly_totals.write.mode(SaveMode.Overwrite).saveAsTable("reid7_transport_weather_monthly")

//bring everything together on a daily level

//first, monthly averages to join on daily
val monthly_avgs= spark.sql(
  """select dd.year, dd.month, cast(avg(trip_duration/total_trips) as int) as avg_div_duration,
    cast(avg(total_trips) as int) as avg_div_trips, cast(avg(total_bus)/count(1) as int) as avg_bus_rides,
    cast(avg(total_rail)/count(1) as int) as avg_train_rides, cast(avg(total_rides)/count(1) as int) as avg_total_rides from
    divvy_daily dd join cta_mo cta on dd.month = cta.month and dd.year = cta.year group by dd.month, dd.year;""")
monthly_avgs.createOrReplaceTempView("monthly_avgs")


val daily_totals = spark.sql(
  """select w.year, w.month, w.day, avg_precipitation, avg_snow,
    avg_temp, trip_duration, total_trips, subscribers, non_subscribers,
    average_age, total_bus, total_rail, total_rides
    from weather_daily w join divvy_daily d join cta c on w.month = c.month and
    w.year = c.year and w.day = c.day and w.month = d.month and w.year = d.year and
    w.day = d.day order by w.year, w.month, w.day""")

daily_totals.createOrReplaceTempView("daily_totals")
val transport_weather = spark.sql(
  """ select d.year || "-" || d.month || "-" || d.day as date_key, d.year,
      d.month, d.day, avg_precipitation,
    if(avg_precipitation > 0.0 and avg_precipitation <0.04, "Slight",
    if(avg_precipitation>= 0.04 and avg_precipitation<0.8, "Moderate",
    if(avg_precipitation>= 0.8 and avg_precipitation<1.5, "Heavy",
    if(avg_precipitation >=1.5, "Very Heavy", "None")))) as precip_cat,
    avg_snow,
    if(avg_snow > 0.0 and avg_snow <1.0, "Slight",
    if(avg_snow >= 1.0 and avg_snow <3.0, "Moderate",
    if(avg_snow>= 3.0, "Heavy", "None"))) as snow_cat,
    avg_temp, trip_duration,
    avg_div_duration as avg_mo_duration, total_trips, avg_div_trips as avg_mo_trips, subscribers,
    non_subscribers, average_age, total_bus, avg_bus_rides as avg_mo_bus, total_rail, avg_train_rides as avg_mo_train,
    total_rides, avg_total_rides as avg_mo_rides from daily_totals d join monthly_avgs m
    on d.month = m.month and d.year = m.year order by d.year, d.month, d.day""")
transport_weather.createOrReplaceTempView("transport_weather")

transport_weather.write.mode(SaveMode.Overwrite).saveAsTable("reid7_transport_weather_daily")
