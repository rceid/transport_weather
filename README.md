# transport_weather

# This repository contains code for Raymond Eid's project for MPCS 53014 - Big Data Application Architecture.

## 0. Video Overview
The video at this [link](https://www.loom.com/share/9f9fd4db9b854d2f9e5ada41c6b983ef) gives and overview of the application interface as well as the batch, serving, and speed layers. Machine learning models are briefly discussed as well.
___

## 1. App and speed layer info:


Deployed application may be found [here](http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3707/home.html)

If load balancers are down, you may access the quick deployment [here](http://ec2-3-15-219-66.us-east-2.compute.amazonaws.com:3707/home.html)

To run speedlayer from EMR terminal:
```
$cd reid7/processMonth/target

$spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class StreamDivvy uber-SL-divvy-1.0-SNAPSHOT.jar b-1.mpcs53014-kafka.fwx2ly.c4.kafka.useast-2.amazonaws.com:9092,b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092
```
____

## 2. Data collection


Data were collected from the City of Chicago Data Portal ([CTA](https://data.cityofchicago.org/Transportation/CTA-Ridership-Daily-Boarding-Totals/6iiy-9s97) and [Divvy](https://data.cityofchicago.org/Transportation/CTA-Ridership-Daily-Boarding-Totals/6iiy-9s97)) and the US [National Oceanic and Atmospheric Administration](https://www.ncdc.noaa.gov/cdo-web/search;jsessionid=9AB2C2CFD9A81924521705D5879AC26B).

City of Chicago data was curled and piped into Hadoop Distributed File System (HDFS) directly, while weather was obtained form the noaa website and manually downloaded in chunks locally before sending to HDFS. Weather files are in the *weather* directory.

Running the following command from the root directory will curl and send all files to HDFS:
```
$./curl_to_HDFS.sh
```

____

## 3. HDFS to Apache Hive
There is one file for each of the three data sources, all located in the *hive_hbase_spark* directory, which move the .csv files from HDFS to Hive:

- *create_CTA_daily.hql*
- *create_divvy.hql* 
- *create_weather.hql*

Each file processes the .csv into a Hive table, then creates a subsequent Optimized Row Columnar (ORC) table to store and read the data more efficiently. 
____

## 4. Data Cleaning with Scala in Spark
Also located in the *hive_hbase_spark*, *spark_clean_join.scala* contains the commands to clean  weather, public transit, and Divvy bike share Hive tables and group them on a daily and monthly level for application views. Cleaned tables are saved directly into Hive with this script.
____
## 5. Send Hive tables to HBase
The last file in the *hive_hbase_spark* directory is *hive_to_hbase.hql*. The script creates a Hive table linked to a new HBase table, serializes the data in Hive and reserializes it in HBase. The HBase tables will interface will the web application and be queried in the appropriate views.

The tables include a monthly views of public transit, bikeshare, and weather info, as well as daily views. They span 2014 to 2019.
____
## 6. Speed Layer with Scala
The directory *speed_layer* contains the script to create the uber-jar to be run from the EMR terminal to ingest data from the submit form of the application.

The script takes the data submitted, processes it as a scala object, then adds the month as a row to the HBase table containing monthly transport and weather data. 
____
## 7. Regression Models Using Transport and Weather Data
The directory *sparkML* contains the two files which create two Machine Learning models and upload them to Hive and HBase. 

The file *spark_ml.scala* contains the scala code that creates the ML models used to predict Divvy Trip duration. The script combines divvy and weather data on a daily level, formats the categorical variables splits the training and testing data 80/20, spearately fits a Linear Regression and Random Forest Regression to three different subsets of features, and exports model metrics to Hive. The three different subsets are i) date, divvy rider, and weather features; ii) rider and weather features; and iii) just weather features. Evaluation metrics include R<sup>2</sup> and Root Mean Squared Error.

Features are broken down as follows:

- Weather features: Average precipitation, snow, and temperature
- Rider features: Subscriber or not, aender, age
- Date features: Year, month, year-month interaction. Interaction not used in random forest as distinct values outnumber model's buckets.

File *ml_to_hbase.hql* subsequently takes the Hive table and uploads it to HBase, along with an "index" table for distinct model types used for the ML view's dropdown menu in the app.
___
## 8. The Web Application
Starting from the home page, the user has four views to select:

**i) Explore Monthly Divvy and CTA usage with weather by year**:

The user selects a year and is displayed the available months as rows. This view also presents the user with weather information, as well as Divvy trip and CTA usage stats. This view allows the user to explore trends in Divvy and CTA use by month while keeping weather in mind.

**ii) Explore Divvy and CTA usage with weather by precipitation average**: 

The user selects a precipitation category (codes below) and can explore percent difference in Divvy and CTA use, displayed on a daily level, as they pertain to their month's average. A user may anticipate how busy the L or buses will be as well as the availability of Divvy bikes based on similar days in the past.

Category | Range
--- | --- 
None | 0 inches
Slight | Greater than 0 and less than 0.04 inches
Moderate | Greater than or equal to 0.04 and less than 0.8 inches
Heavy | Greater than or equal to 0.8 and less than 1.5 inches
Very Heavy | Greater or equal to 1.5 inches

.

**iii) Explore Divvy and CTA usage with weather by snowfall average**: 

Similar to the precipitation view, this allows users to anticipate CTA and Divvy use based on average daily snowfall. Codes are as follows:

Category | Range
--- | --- 
None | 0 inches
Slight | Greater than 0 and less than 1.0 inches
Moderate | Greater than or equal to 1.0 and less than 3.0 inches
Heavy | Greater than or equal to 3.0 
 
 .


**iv) Submit Monthly Divvy Data**: 
 
 The user can submit ridership data for a non-prexisting year/month combination, which is handled and processed by the speed layer. Entries will be viewable on the page *i) Explore Monthly Divvy and CTA usage with weather by year*.

 **iv) View ML Models Predicting Divvy Trip Duration**: 

 The user can view the two regression models using the batch layer data. Models are described in part 7) Regression Models Using Transport and Weather Data.
 ___
 
Many thanks to to Genaro Servín, who made the application's background picture freely available at: https://www.pexels.com/photo/person-riding-a-bicycle-during-rainy-day-763398/