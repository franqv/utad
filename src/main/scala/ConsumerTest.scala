import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
// import com.datastax.driver.core.Cluster
object ConsumerTest extends App {
  val topic = "taxiTopic"
  val conf = new SparkConf().setAppName("streamingApp").setMaster("local[4]")
    //.set("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    //.set("spark.app.id", "Spark")
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
  // val values = stream.map(record => parse(record.value()).values.asInstanceOf[Map[String, Any]])
  // val infoDS = stream.map(record => (record.topic, record.key, record.value))
  stream.foreachRDD { rdd =>
    println (rdd)
  }
  ssc.start()
  ssc.awaitTermination()
}


