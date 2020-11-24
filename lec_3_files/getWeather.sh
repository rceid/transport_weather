#!/bin/bash
# Note. I already ran on cluster. Please don't run again
mkdir weatherData
cd weatherData
year=2000
while [ $year -le 2019 ]
do
    wget ftp://ftp.ncdc.noaa.gov/pub/data/gsod/$year/gsod_$year.tar
    (( year++ ))
done
