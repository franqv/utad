import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.{SQLContext}

object sparkConsumer extends App {
  val topic = "taxiTopic"
  val filePath = "/home/francisco/Documentos/1-UTAD_TFM/taxi/yellow_tripdata_2020-01.csv"
  val separator = ","

  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[4]")
    .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("spark.app.id", "Spark")
    .set("spark.cassandra.connection.host", "localhost:7000")

  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "consumer-spark",
    "auto.offset.reset" -> "latest",
  )

  val topics = Array(topic)
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )

  // val sqlContext = new SQLContext(sc)

  // import sqlContext.implicits._

  // val values = stream.map(record => (record.key(), parse(record.value()).values.asInstanceOf[Map[String, Any]]))
  val values = stream.map(record => parse(record.value()).values.asInstanceOf[Map[String, Any]])

  values.foreachRDD(rdd => if (!rdd.partitions.isEmpty)
    rdd.map(x => (("VendorID"), x("tpep_pickup_datetime"), x("tpep_dropoff_datetime"), x("passenger_count"), x("trip_distance"), x("RatecodeID"), x("store_and_fwd_flag"), x("PULocationID"), x("DOLocationID"), x("payment_type"), x("fare_amount"), x("extra"), x("mta_tax"), x("tip_amount"), x("tolls_amount"), x("improvement_surcharge"), x("total_amount"), x("congestion_surcharge")))
      .toDF("VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count","trip_distance","RatecodeID","store_and_fwd_flag","PULocationID","DOLocationID","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge")
      .write.format("parquet").mode("append").save("/taxisDF/"))

  ssc.start()
  ssc.awaitTermination()
}