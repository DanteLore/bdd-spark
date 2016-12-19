import org.apache.spark.sql.SparkSession

object Spark {
  val spark = SparkSession
    .builder()
    .appName("BDD Example")
    .master("local[8]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.default.parallelism", "8")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
}
