import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.{SQLContext}

object sparkPruebas extends App {
  val topic = "taxiTopic"
  val filePath = "/home/francisco/Documentos/1-UTAD_TFM/taxi/yellow_tripdata_2020-01.csv"
  val separator = ","

  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[4]")
    .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    .set("spark.app.id", "Spark")

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

  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val values = stream.map(record => (record.key(), parse(record.value()).values.asInstanceOf[Map[String, Double]]))

  values.foreachRDD(rdd => if (!rdd.partitions.isEmpty)
    rdd.map(x => (x._1, x._2("diff_pickup_dropoff"), x._2("passenger_count"), x._2("trip_distance"), x._2("total_amount")))
      .toDF("day", "diff_pickup_dropoff", "passenger_count", "trip_distance", "total_amount")
      .write.format("parquet").mode("append").save("/taxisDF/"))

  stream.map(record => (record.key, Tuple5(
    parse(record.value()).values.asInstanceOf[Map[String, Double]]("diff_pickup_dropoff"),
    parse(record.value()).values.asInstanceOf[Map[String, Double]]("passenger_count"),
    parse(record.value()).values.asInstanceOf[Map[String, Double]]("trip_distance"),
    parse(record.value()).values.asInstanceOf[Map[String, Double]]("total_amount"),
    1)))
    .foreachRDD(rdd => rdd.reduceByKey((x, y) => (x._1.toFloat + y._1.toFloat,
      x._2.toFloat + y._2.toFloat,
      x._3.toFloat + y._3.toFloat,
      x._4.toFloat + y._4.toFloat,
      x._5 + y._5))
      .map(x => (x._1,
        x._2._1 / x._2._5,
        x._2._2 / x._2._5,
        x._2._3 / x._2._5,
        x._2._4 / x._2._5))
      .toDF("Day", "travelTimeAvg(Min)", "passengerAvg", "tripDistanceAvg(Miles)", "totalAmountAvg(USD)")
      .withColumn("Day", to_date($"Day", "yyy-MM-dd"))) //.write.mode("append").mongo())

  ssc.start()
  ssc.awaitTermination()
}