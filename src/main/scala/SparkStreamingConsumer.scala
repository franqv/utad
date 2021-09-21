import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
// import com.datastax.driver.core.Cluster

object sparkConsumer extends App {
  val topic = "taxiTopic"
  val separator = ","

  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[*]")
    .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .set("mapreduce.fileoutputcommitter.algorithm.version", "2")
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
    "enable.auto.commit" -> "true",
    "auto.commit.interval.ms" -> "1000",
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

  // val spark = new SparkSession(sc)
  // import spark.implicits._

  values.foreachRDD { rdd =>
    if (!rdd.partitions.isEmpty) {
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      val taxiDF = rdd.map(x => (x("VendorID"), x("tpep_pickup_datetime"), x("tpep_dropoff_datetime"), x("passenger_count"), x("trip_distance"), x("RatecodeID"), x("store_and_fwd_flag"), x("PULocationID"), x("DOLocationID"), x("payment_type"), x("fare_amount"), x("extra"), x("mta_tax"), x("tip_amount"), x("tolls_amount"), x("improvement_surcharge"), x("total_amount"), x("congestion_surcharge")))
        .toDF("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge")
      taxiDF.write.format("parquet").mode("append").save("/tfm/taxi/")

      spark.sql("CREATE DATABASE IF NOT EXISTS utad.taxis WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')")
      spark.sql("CREATE TABLE IF NOT EXISTS utad.taxis.rawinfo (VendorID Int, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count Int, trip_distance DOUBLE, RatecodeID Int, store_and_fwd_flag String, PULocationID Int, DOLocationID Int, payment_type Int, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE)" +
        " USING cassandra PARTITIONED BY (VendorID)")

      taxiDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "rawinfo", "keyspace" -> "utad.taxis")).save()
      spark.close()
    }
  }

  ssc.start()
  ssc.awaitTermination()
}