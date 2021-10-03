import org.apache.spark.sql.SparkSession


object timeCASSANDRA extends App {
  val spark = SparkSession.builder.appName("timeCASSANDRA")
    .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .config("spark.cassandra.connection.host", "127.0.0.1")
    .config("spark.cassandra.connection.port","9042")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .config("spark.cassandra.auth.username","cassandra")
    .config("spark.cassandra.auth.password","cassandra")
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
    .config("spark.sql.catalog.history", "com.datastax.spark.connector.datasource.CassandraCatalog")
    .master("local[4]").getOrCreate()
  val df = spark.read.format("org.apache.spark.sql.cassandra")
  .options(Map("table" -> "olap_cube", "keyspace" -> "tfm"))
  .load()
  spark.time(df.collect())
  println("Tiempo de ejecución de recoleccion de la tabla olap_cube desde CASSANDRA")
  //df.show()
}

