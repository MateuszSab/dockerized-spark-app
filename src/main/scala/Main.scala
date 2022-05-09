import org.apache.spark.sql.types._

import java.io.File

object Main extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("dockerized-spark-app")
    .master("local[*]")
    .getOrCreate()


  if (args.length < 1) {
    println("ERROR: brak argumentu")
    sys.exit(-1)
  }
  val path = args(0)
  val isDirAvailable = new File(path).exists()
  if (!isDirAvailable) {
    println(s"ERROR: nie ma takiej ścieżki: $path")
    sys.exit(-1)
  }


  val schema = StructType(
    StructField("id", IntegerType, true) ::
      StructField("city", StringType, false) ::
      StructField("country", StringType, false) :: Nil
  )


  val df = spark.readStream.format("csv").options(Map("delimiter" -> ",", "header" -> "true")).schema(schema).load(path)
  df.writeStream.format("console").start.awaitTermination()
}