add jar add jar hdfs:///tmp/spertus/jars/IW-0.0.1-SNAPSHOT.jar;

CREATE EXTERNAL TABLE IF NOT EXISTS WeatherSummary
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
    WITH SERDEPROPERTIES (
      'serialization.class' = 'edu.uchicago.mpcs53013.weatherSummary.WeatherSummary',
      'serialization.format' =  'org.apache.thrift.protocol.TBinaryProtocol')
  STORED AS SEQUENCEFILE 
  LOCATION '/inputs/thriftWeather';

-- Test our table
select * from WeatherSummary limit 5;