import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Spark {
  val conf = new SparkConf()
    .setAppName("MOT Test")
    .setMaster("local[8]")
    .set("spark.default.parallelism", "8")
    .set("spark.sql.shuffle.partitions", "8")

  val sc = new SparkContext(conf)
  LogManager.getRootLogger.setLevel(Level.ERROR)

  val sqlContext = new SQLContext(Spark.sc)
  sqlContext.setConf("spark.sql.shuffle.partitions", "8")
}
