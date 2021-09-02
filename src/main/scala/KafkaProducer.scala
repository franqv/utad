import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.DefaultFormats

import scala.io.Source
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL.WithBigDecimal._

object KafkaProducer extends App {

  val topic = "KafkaProducerTopic"
  val brokers = "localhost:9092"
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducer")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val bufferedSource = Source.fromFile("/home/francisco/Documentos/1-UTAD_TFM/taxi/yellow_tripdata_2020-01.csv")

  // VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,
  // store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,
  // tolls_amount,improvement_surcharge,total_amount,congestion_surcharge

  for (line <- bufferedSource.getLines.drop(1)) {
    val cols = line.split(",").map(_.trim)
    val json = ("VendorID" -> cols(0)) ~
      ("startime" -> cols(1)) ~
      ("stoptime" -> cols(2)) ~
      ("passenger_count" -> cols(3)) ~
      ("trip_distance" -> cols(4)) ~
      ("RatecodeID" -> cols(5)) ~
      ("store_and_fwd_flag" -> cols(6)) ~
      ("PULocationID" -> cols(7)) ~
      ("DOLocationID" -> cols(8)) ~
      ("payment_type" -> cols(9)) ~
      ("fare_amount" -> cols(10)) ~
      ("extra" -> cols(11)) ~
      ("tip_amount" -> cols(12)) ~
      ("tolls_amount" -> cols(13)) ~
      ("improvement_surcharge" -> cols(14)) ~
      ("total_amount" -> cols(15)) ~
      ("congestion_surcharge" -> cols(16))

    implicit val formats: DefaultFormats.type = DefaultFormats

    val msg = compact(render(json))
    val data = new ProducerRecord[String, String](topic, msg)

    producer.send(data)
  }
  bufferedSource.close

  producer.close()
}