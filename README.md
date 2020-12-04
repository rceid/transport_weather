# nibrs_project
This repository contains code for my project for MPCS 53014 - Big Data Application Architecture.

Data were collected from
The City of Chicago Data Portal (CTA and DIvvy)
The Noaa (Chicago weather)

Chicago data was curled and piped into HDFS directly, while weather was obtained form the noaa website (https://www.ncdc.noaa.gov/cdo-web/search) and manually downloaded in chunks locally before sending to HDFS.

Deployed application may be found on:
http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3707/home.html

If load balancers are down, you may access the quick deployment here:
http://ec2-3-15-219-66.us-east-2.compute.amazonaws.com:3707/home.html

run the speedlayer:
spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamDivvy uber-SL-divvy-1.0-SNAPSHOT.jar b-1.mpcs53014-kafka.fwx2ly.c4.kafka.useast-2.amazonaws.com:9092,b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092

