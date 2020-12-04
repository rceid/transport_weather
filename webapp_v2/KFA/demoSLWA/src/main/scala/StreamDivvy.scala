import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.client.Increment


object StreamDivvy {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("latest_weather"))
  val divvyTable = hbaseConnection.getTable(TableName.valueOf("reid7_transport_weather_monthly"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers> 
        |  <brokers> is a list of one or more Kafka brokers
        | 
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamDivvy") //Changed from StreamWeather
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("reid7_transport_weather") //changed from reid7_f_w
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val report = serializedRecords.map(rec => mapper.readValue(rec, classOf[DivvyReport]))

    //My code, checking if the yr-mo row exists
    val date_key = Bytes.toBytes(report.year + "-" + report.month)
    val result = divvyTable.get(new Get(date_key))
    if (result.isEmpty())
    // writing the new row
      val batchStats = reports.map(dr => {
        val put = new Put(date_key))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("month"), Bytes.toBytes(dr.month))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("year"), Bytes.toBytes(dr.year))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("trip_duration"), Bytes.toBytes(dr.trip_duration))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("subscribers"), Bytes.toBytes(dr.subscriber))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("non_subscribers"), Bytes.toBytes(dr.subscriber))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("avg_precipitation"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("avg_snow"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("avg_temp"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("total_trips"), Bytes.toBytes(1))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("average_age"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("total_bus"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("total_rail"), Bytes.toBytes("-"))
        put.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("total_rides"), Bytes.toBytes("-"))
        divvyTable.put(put)
      })
      batchStats.print()
    else
      //Increment the row if it exists
      val inc = new Increment(date_key)
      inc.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("trip_duration"), result.trip_duration)
      inc.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("subscribers"), result.subscriber)
      inc.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("non_subscribers"), result.subscriber)
      inc.addColumn(Bytes.toBytes("stat"), Bytes.toBytes("total_trips"), 1)
      divvyTable.increment(inc)


    
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
