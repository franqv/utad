import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.parse

//import org.apache.spark.sql.cassandra._
//import com.datastax.spark.connector.cql.CassandraConnectorConf

// import com.datastax.driver.core.Cluster

object ConsumerTest extends App {
  val topic = "taxiTopic"
  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[4]")
    .set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .set("spark.app.id", "Spark")
    .set("spark.sql.warehouse.dir", "")
    .set("spark.cassandra.connection.host", "127.0.0.1")  // 127.0.0.1     localhost
    .set("spark.cassandra.connection.port","9042")   // probar con 7000 si no funciona
    .set("spark.cassandra.auth.username","cassandra")
    .set("spark.cassandra.auth.password","cassandra")
    .set("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .set("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")


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

  // val sqlContext = new SQLContext(sc)

  // import sqlContext.implicits._

  // val values = stream.map(record => (record.key(), parse(record.value()).values.asInstanceOf[Map[String, Any]]))

  val values = stream.map(record => parse(record.value()).values.asInstanceOf[Map[String, String]])
  // val infoDS = stream.map(record => (record.topic, record.key, record.value))

  //stream.foreachRDD { rdd =>
  //  println ("Values: ")
  //  println (values)
  //}

  // val spark = new SparkSession(sc)
  // import spark.implicits._

  values.foreachRDD { rdd =>
    if (!rdd.partitions.isEmpty) {
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      /*
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf)
      .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port","7000")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .getOrCreate()
      */
      import spark.implicits._
      println (rdd)
      val taxiDF = rdd.map(x => (x("VendorID"), x("tpep_pickup_datetime"), x("tpep_dropoff_datetime"), x("passenger_count"), x("trip_distance"), x("RatecodeID"), x("store_and_fwd_flag"), x("PULocationID"), x("DOLocationID"), x("payment_type"), x("fare_amount"), x("extra"), x("mta_tax"), x("tip_amount"), x("tolls_amount"), x("improvement_surcharge"), x("total_amount"), x("congestion_surcharge")))
        .toDF("vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge")
      taxiDF.write.format("parquet").mode("append").save("/tfm/taxi/")
      println ("taxiDF: ")
      println (taxiDF)

      // spark.sql("CREATE DATABASE IF NOT EXISTS tfm WITH DBPROPERTIES (class='SimpleStrategy',replication_factor='1')  USING cassandra ")

      /*
      spark.sql("CREATE KEYSPACE IF NOT EXISTS tfm WITH REPLICATION ={ 'class':'SimpleStrategy','replication_factor':'1'}")
      spark.sql("CREATE TABLE IF NOT EXISTS tfm.rawinfo (VendorID Int, tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, passenger_count Int, trip_distance DOUBLE, RatecodeID Int, store_and_fwd_flag String, PULocationID Int, DOLocationID Int, payment_type Int, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE)" +
        " USING cassandra PARTITIONED BY (tpep_pickup_datetime)")
       */

      taxiDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "rawinfo", "keyspace" -> "tfm")).mode(SaveMode.Append).save()

      val key_value = rdd.map(
        x => (x("tpep_pickup_datetime").split(" ")(0)+"-"+x("tpep_pickup_datetime").split(" ")(1).split(":")(0),
          (
            x("tpep_dropoff_datetime").split(" ")(1).split(":")(0).toFloat*3600+x("tpep_dropoff_datetime").split(" ")(1).split(":")(1).toFloat*60+x("tpep_dropoff_datetime").split(" ")(1).split(":")(2).toFloat-x("tpep_pickup_datetime").split(" ")(1).split(":")(0).toFloat*3600-x("tpep_pickup_datetime").split(" ")(1).split(":")(1).toFloat*60-x("tpep_pickup_datetime").split(" ")(1).split(":")(2).toFloat,
            x("passenger_count").toFloat,
            x("trip_distance").toFloat,
            x("payment_type"),
            x("total_amount").toFloat,
            1
          )
        )
      )
      val olap_cube = key_value.reduceByKey(
        (x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6)
      ).mapValues {
        case (sum1, sum2, sum3, total, sum4, count) => ((1.0 * sum1) / count, (1.0 * sum2) / count, (1.0 * sum3) / count, total, (1.0 * sum4) / count)
      }.map(
        x => (x._1,x._2._1,x._2._2,x._2._3,x._2._4,x._2._5)
      ).toDF(
        "dia_hora","travel_time","avg_passengers","avg_trip_distance","type_payments","avg_total_amount"
      )
      println (olap_cube)
      olap_cube.show(5)

      olap_cube.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "olap_cube", "keyspace" -> "tfm")).mode(SaveMode.Append).save()

      // spark.close()
    }
  }
  ssc.start()
  ssc.awaitTermination()
}


