# transport_weather

## This repository contains code for Raymond Eid's project for MPCS 53014 - Big Data Application Architecture.

Data were collected from:
The City of Chicago Data Portal ([CTA](https://data.cityofchicago.org/Transportation/CTA-Ridership-Daily-Boarding-Totals/6iiy-9s97) and [Divvy](https://data.cityofchicago.org/Transportation/CTA-Ridership-Daily-Boarding-Totals/6iiy-9s97)])
The [NOAA] (https://www.ncdc.noaa.gov/cdo-web/search;jsessionid=9AB2C2CFD9A81924521705D5879AC26B)

Chicago data was curled and piped into HDFS directly, while weather was obtained form the noaa website (https://www.ncdc.noaa.gov/cdo-web/search) and manually downloaded in chunks locally before sending to HDFS. Weather files are in the 'weather' directory. 
Running the following command from the root directory will curl and send all files to HDFS:
$./curl_to_HDFS.sh

Deployed application may be found on:
http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3707/home.html

If load balancers are down, you may access the quick deployment here:
http://ec2-3-15-219-66.us-east-2.compute.amazonaws.com:3707/home.html

run the speedlayer from EMR terminal:
$cd reid7/processMonth/target
$spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamDivvy uber-SL-divvy-1.0-SNAPSHOT.jar b-1.mpcs53014-kafka.fwx2ly.c4.kafka.useast-2.amazonaws.com:9092,b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092

