import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object timeHDFS extends App {
  val spark = SparkSession.builder.appName("timeHDFS")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:8020")
    .master("local[4]").getOrCreate()

  val df = spark.read.format("parquet").parquet("/tfm/taxi/")
  val df_taxi = df.select(
    date_trunc("HOUR", col("tpep_pickup_datetime")).as("dia_hora"),
    (hour(col("tpep_dropoff_datetime"))*3600+minute(col("tpep_dropoff_datetime"))*60+second(col("tpep_dropoff_datetime"))-hour(col("tpep_pickup_datetime"))*3600-minute(col("tpep_pickup_datetime"))*60-second(col("tpep_pickup_datetime"))).as("travel_time"),
    (col("passenger_count")).as("passenger_count"),
    (col("trip_distance")).as("trip_distance"),
    (col("payment_type")).as("payment_type"),
    (col("total_amount")).as("total_amount")
  )

  val df_taxi_avg = df_taxi.groupBy(col("dia_hora")).agg(
    avg("travel_time").as("avg_travel_time"),
    avg(col("passenger_count")).as("avg_passengers"),
    avg(col("trip_distance")).as("avg_trip_distance"),
    collect_list(col("payment_type")).as("type_payments"),
    avg(col("total_amount")).as("avg_total_amount"),
    count(col("payment_type")).as("n_count")
  )

  spark.time(df_taxi_avg.collect())
  println("Tiempo de ejecución de recoleccion y transformación de datos desde HDFS")
  //df_taxi_avg.agg(count(col("dia_hora")).as("Numero_Filas")).show()
  //df_taxi_avg.show()
}