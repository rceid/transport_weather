-- Make sure you have put stations.txt in hdfs://input/stations
CREATE EXTERNAL TABLE stations (code STRING, name STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\,",
   "quoteChar"     = "\""
)
STORED AS TEXTFILE
  location '/inputs/stations';