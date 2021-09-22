import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.Source
import com.google.gson.Gson


object KafkaProducer extends App {

  val topic = "taxiTopic"
  //val brokers = "kafka1:19092"//
  val brokers = "localhost:9092"
  //val filePath = "/home/francisco/Documentos/1-UTAD_TFM/taxi/yellow_tripdata_2020-01.csv"
  val filePath = "/home/mafernandez/Descargas/yellow_tripdata_2020-01.csv"
  val props = new Properties()

  props.put("bootstrap.servers", brokers)
  props.put("client.id", "ScalaProducer")
  props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

  //val producer = new KafkaProducer[String, String](props)
  val bufferedSource = Source.fromFile(filePath)

  // VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,
  // store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,
  // tolls_amount,improvement_surcharge,total_amount,congestion_surcharge

  case class taxiLine(VendorID: String, tpep_pickup_datetime: String, tpep_dropoff_datetime: String, passenger_count: String, trip_distance: String, RatecodeID: String, store_and_fwd_flag: String, PULocationID: String, DOLocationID: String, payment_type: String, fare_amount: String, extra: String, mta_tax: String, tip_amount: String, tolls_amount: String, improvement_surcharge: String, total_amount: String, congestion_surcharge: String)

  for (line <- bufferedSource.getLines.drop(1)) {
    val cols = line.split(",") //.map(_.trim)
    val dataString = taxiLine(cols(0),cols(1),cols(2),cols(3),cols(4),cols(5),cols(6),cols(7),cols(8),cols(9),cols(10),cols(11),cols(12),cols(13),cols(14),cols(15),cols(16),cols(17))

    val gson = new Gson
    val messaje = gson.toJson(dataString)

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic,messaje)
    producer.send(record)
    producer.close()
  }
  bufferedSource.close

//  producer.close()
}