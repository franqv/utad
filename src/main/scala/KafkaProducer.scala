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
    val json = ("tripduration" -> cols(0)) ~
      ("VendorID" -> cols(1)) ~
      ("startime" -> cols(2)) ~
      ("stoptime" -> cols(3)) ~
      ("passenger_count" -> cols(4)) ~
      ("trip_distance" -> cols(5)) ~
      ("RatecodeID" -> cols(6)) ~
      ("store_and_fwd_flag" -> cols(7)) ~
      ("PULocationID" -> cols(8)) ~
      ("DOLocationID" -> cols(9)) ~
      ("payment_type" -> cols(10)) ~
      ("fare_amount" -> cols(11)) ~
      ("extra" -> cols(12)) ~
      ("tip_amount" -> cols(13)) ~
      ("tolls_amount" -> cols(14)) ~
      ("improvement_surcharge" -> cols(15)) ~
      ("total_amount" -> cols(16)) ~
      ("congestion_surcharge" -> cols(17))

    implicit val formats: DefaultFormats.type = DefaultFormats

    val msg = compact(render(json))
    val data = new ProducerRecord[String, String](topic, msg)

    producer.send(data)
  }
  bufferedSource.close

  producer.close()
}