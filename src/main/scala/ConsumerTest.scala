import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse
// import com.datastax.driver.core.Cluster
object ConsumerTest extends App {
  val topic = "taxiTopic"
  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[4]")
    .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .set("spark.app.id", "Spark")
    .set("spark.sql.warehouse.dir", "")
    .set("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .set("spark.cassandra.connection.host", "localhost:7000")
    .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Seconds(1))
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "group.id" -> "consumer-spark",
    //"enable.auto.commit" -> "true",
    //"auto.commit.interval.ms" -> "1000",
    //"auto.offset.reset" -> "latest",
  )
  val topics = Array(topic)
  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    PreferConsistent,
    Subscribe[String, String](topics, kafkaParams)
  )
  val values = stream.map(record => parse(record.value()).values.asInstanceOf[Map[String, String]])
  // val infoDS = stream.map(record => (record.topic, record.key, record.value))
  stream.foreachRDD { rdd =>
    println ("Values: ")
    println (values)
  }

  values.foreachRDD { rdd =>
    if (!rdd.partitions.isEmpty) {
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      println (rdd)
      val taxiDF = rdd.map(x => (x("VendorID"), x("tpep_pickup_datetime"), x("tpep_dropoff_datetime"), x("passenger_count"), x("trip_distance"), x("RatecodeID"), x("store_and_fwd_flag"), x("PULocationID"), x("DOLocationID"), x("payment_type"), x("fare_amount"), x("extra"), x("mta_tax"), x("tip_amount"), x("tolls_amount"), x("improvement_surcharge"), x("total_amount"), x("congestion_surcharge")))
        .toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge")
      taxiDF.write.format("parquet").mode("append").save("/tfm/taxi/")
      println ("taxiDF: ")
      println (taxiDF)

      // spark.close()
    }
  }
  ssc.start()
  ssc.awaitTermination()
}


